from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse
from typing import Optional, Dict
import os
import json
import uuid
import asyncio
import time
import hashlib
import clipper
from supabase_client import supabase, upload_clip_to_storage, delete_old_clips, get_signed_url, get_user_clips


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR  = "uploads"
OUTPUT_DIR  = "output"
CLEANUP_INT = int(os.getenv("CLEANUP_INTERVAL", 1800))   # run cleanup every 30 min
JOB_TTL     = int(os.getenv("JOB_TTL_SECONDS",  7200))   # forget job records after 2 hrs

for d in [UPLOAD_DIR, OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

# ─────────────────────────────────────────────
# In-memory stores
# ─────────────────────────────────────────────
jobs: Dict[str, dict] = {}
_clip_cache: Dict[str, list] = {}
_last_cleanup: float = time.time()

# SSE subscribers: job_id → list of asyncio.Queue
_sse_subscribers: Dict[str, list] = {}

# One-time stream tokens: token → {"user_id", "expires"}. The bearer JWT
# never goes in a URL (query strings leak via logs/history); the client
# exchanges it for a short-lived single-use token instead.
_stream_tokens: Dict[str, dict] = {}
STREAM_TOKEN_TTL = 300


def _consume_stream_token(token: str) -> Optional[str]:
    """Validate and burn a one-time stream token. Returns user_id or None."""
    now = time.time()
    # Drop expired tokens opportunistically
    for t in [t for t, v in _stream_tokens.items() if v["expires"] < now]:
        _stream_tokens.pop(t, None)
    entry = _stream_tokens.pop(token, None)
    if entry and entry["expires"] >= now:
        return entry["user_id"]
    return None


def _notify_job(job_id: str, event_data: dict):
    """Push an SSE event to all subscribers of a job."""
    for queue in _sse_subscribers.get(job_id, []):
        try:
            queue.put_nowait(event_data)
        except asyncio.QueueFull:
            pass

# ─────────────────────────────────────────────
# Auth — validate Supabase JWT on protected routes
# ─────────────────────────────────────────────
bearer = HTTPBearer()

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(bearer)) -> dict:
    """
    Verify the JWT that Supabase issues on login.
    Returns the decoded user payload so routes can access user_id.
    """
    token = credentials.credentials
    try:
        # supabase-py verifies signature + expiry against the project's JWT secret
        user = supabase.auth.get_user(token)
        if not user or not user.user:
            raise HTTPException(status_code=401, detail="Invalid or expired token")
        return {"user_id": user.user.id, "email": user.user.email}
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Auth error: {str(e)}")


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def _video_cache_key(url: str, instructions: Optional[str], user_id: str,
                     clip_style: str = "auto", caption_style: str = "default",
                     clip_count: Optional[int] = None,
                     min_clip_length: Optional[int] = None,
                     max_clip_length: Optional[int] = None) -> str:
    """Cache key scoped per user so different users don't share clips."""
    raw = (f"{user_id}||{url}||{instructions or ''}||{clip_style}||{caption_style}"
           f"||{clip_count or ''}||{min_clip_length or ''}||{max_clip_length or ''}")
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


async def _maybe_cleanup():
    """Trigger Supabase storage cleanup at most once per CLEANUP_INT."""
    global _last_cleanup
    if time.time() - _last_cleanup > CLEANUP_INT:
        _last_cleanup = time.time()
        loop = asyncio.get_event_loop()
        deleted = await loop.run_in_executor(None, delete_old_clips)
        # Also purge stale in-memory job records
        now = time.time()
        stale = [jid for jid, j in jobs.items() if j.get("created_at", now) < now - JOB_TTL]
        for jid in stale:
            jobs.pop(jid, None)
        if deleted or stale:
            print(f"[cleanup] {deleted} storage file(s) deleted, {len(stale)} job record(s) purged")


# ─────────────────────────────────────────────
# Background tasks
# ─────────────────────────────────────────────

async def process_video_task(
    job_id: str,
    video_path: str,
    instructions: Optional[str],
    user_id: str,
    info: Optional[dict] = None,
    captions: bool = True,
    cache_key: Optional[str] = None,
    clip_style: str = "auto",
    caption_style: str = "default",
    clip_count: Optional[int] = None,
    min_clip_length: Optional[int] = None,
    max_clip_length: Optional[int] = None,
):
    loop = asyncio.get_event_loop()
    try:
        # ── 1. Analyse ────────────────────────────────────────────────────────
        jobs[job_id]["status"] = "analyzing"
        _notify_job(job_id, {"status": "analyzing", "detail": "Analyzing video for viral moments..."})
        clips_metadata = await loop.run_in_executor(
            None, clipper.analyze_video, video_path, instructions, info,
            clip_style, clip_count, min_clip_length, max_clip_length
        )

        if not clips_metadata:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"]  = "No viral moments found in this video"
            _notify_job(job_id, {"status": "failed", "error": "No viral moments found in this video"})
            return

        _notify_job(job_id, {"status": "analyzing", "detail": f"Found {len(clips_metadata)} potential clips"})

        # ── 2. Render clips locally ───────────────────────────────────────────
        jobs[job_id]["status"] = "clipping"
        _notify_job(job_id, {"status": "clipping", "detail": f"Rendering {len(clips_metadata)} clips...", "total_clips": len(clips_metadata)})
        clips, render_failures = await loop.run_in_executor(
            None, clipper.create_clips, video_path, clips_metadata, OUTPUT_DIR,
            captions, caption_style
        )

        # ── 3. Upload each clip to Supabase Storage ───────────────────────────
        jobs[job_id]["status"] = "uploading"
        _notify_job(job_id, {"status": "uploading", "detail": f"Uploading {len(clips)} clips...", "rendered": len(clips)})
        results = []
        upload_errors = []
        source_url = jobs[job_id].get("url", "")

        for clip in clips:
            try:
                local_path  = os.path.join(OUTPUT_DIR, clip["filename"])
                storage_path = await loop.run_in_executor(
                    None,
                    upload_clip_to_storage,
                    local_path,
                    user_id,
                    job_id,
                    clip.get("description", ""),
                    source_url,
                    clip.get("start_time", 0),
                    clip.get("end_time", 0),
                    clip.get("hook", ""),
                    clip.get("virality_score", 0),
                    clip.get("clip_type", ""),
                )
                signed_url = await loop.run_in_executor(
                    None, get_signed_url, storage_path
                )

                results.append({
                    **clip,
                    "url":            signed_url,
                    "video_url":      signed_url,
                    "src":            signed_url,
                    "storage_path":   storage_path,
                    "hook":           clip.get("hook", ""),
                    "virality_score": clip.get("virality_score", 0),
                    "clip_type":      clip.get("clip_type", ""),
                })

                try:
                    os.remove(local_path)
                except OSError:
                    pass
            except Exception as e:
                upload_errors.append({"filename": clip["filename"], "error": str(e)})
                print(f"  [upload] Failed to upload {clip['filename']}: {e}")

        # ── 4. Determine final status (partial results supported) ─────────────
        total_requested = len(clips_metadata)
        all_errors = render_failures + upload_errors

        if results:
            jobs[job_id]["status"]  = "completed"
            jobs[job_id]["results"] = results
            if all_errors:
                jobs[job_id]["warnings"] = f"{len(all_errors)} of {total_requested} clips failed"
                jobs[job_id]["failed_clips"] = all_errors
            if cache_key:
                _clip_cache[cache_key] = results
            _notify_job(job_id, {
                "status": "completed",
                "results": results,
                "warnings": jobs[job_id].get("warnings"),
            })
        else:
            err_msg = f"All {total_requested} clips failed to render/upload"
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"]  = err_msg
            jobs[job_id]["failed_clips"] = all_errors
            _notify_job(job_id, {"status": "failed", "error": err_msg})

        # Delete source video
        try:
            os.remove(video_path)
        except OSError:
            pass

    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"]  = str(e)
        _notify_job(job_id, {"status": "failed", "error": str(e)})
        print(f"[job {job_id}] failed: {e}")


async def download_and_process(
    job_id: str,
    url: str,
    instructions: Optional[str],
    user_id: str,
    cache_key: str,
    captions: bool = True,
    clip_style: str = "auto",
    caption_style: str = "default",
    clip_count: Optional[int] = None,
    min_clip_length: Optional[int] = None,
    max_clip_length: Optional[int] = None,
):
    loop = asyncio.get_event_loop()
    try:
        jobs[job_id]["status"] = "downloading"
        _notify_job(job_id, {"status": "downloading", "detail": "Downloading video..."})
        video_path, info = await loop.run_in_executor(
            None, clipper.download_video, url, UPLOAD_DIR
        )
        jobs[job_id]["video_path"] = video_path
        _notify_job(job_id, {"status": "downloading", "detail": "Download complete"})
        await process_video_task(
            job_id, video_path, instructions, user_id, info, captions, cache_key,
            clip_style, caption_style, clip_count, min_clip_length, max_clip_length
        )
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"]  = str(e)
        _notify_job(job_id, {"status": "failed", "error": str(e)})


# ─────────────────────────────────────────────
# Public routes (no auth)
# ─────────────────────────────────────────────

@app.get("/")
async def root():
    return {"message": "Clipwave API is running"}


@app.post("/auth/signup")
async def signup(email: str = Form(...), password: str = Form(...)):
    """Create a new Supabase user account."""
    try:
        res = supabase.auth.sign_up({"email": email, "password": password})
        return {"message": "Signup successful — check your email to confirm", "user_id": res.user.id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/auth/login")
async def login(email: str = Form(...), password: str = Form(...)):
    """Login and return access + refresh tokens."""
    try:
        res = supabase.auth.sign_in_with_password({"email": email, "password": password})
        return {
            "access_token":  res.session.access_token,
            "refresh_token": res.session.refresh_token,
            "user_id":       res.user.id,
            "email":         res.user.email,
        }
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@app.post("/auth/refresh")
async def refresh_token(refresh_token: str = Form(...)):
    """Exchange a refresh token for a new access token."""
    try:
        res = supabase.auth.refresh_session(refresh_token)
        return {
            "access_token":  res.session.access_token,
            "refresh_token": res.session.refresh_token,
        }
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


# ─────────────────────────────────────────────
# Protected routes (require Bearer token)
# ─────────────────────────────────────────────

@app.post("/process-url")
async def process_url(
    background_tasks: BackgroundTasks,
    url: str = Form(...),
    instructions: Optional[str] = Form(None),
    captions: bool = Form(True),
    clip_style: str = Form("auto"),
    caption_style: str = Form("default"),
    clip_count: Optional[int] = Form(None),
    min_clip_length: Optional[int] = Form(None),
    max_clip_length: Optional[int] = Form(None),
    user: dict = Depends(get_current_user),
):
    # Validate style params
    if clip_style not in clipper.CLIP_STYLES:
        raise HTTPException(status_code=400, detail=f"Invalid clip_style. Choose from: {list(clipper.CLIP_STYLES.keys())}")
    if caption_style not in clipper.CAPTION_PRESETS:
        raise HTTPException(status_code=400, detail=f"Invalid caption_style. Choose from: {list(clipper.CAPTION_PRESETS.keys())}")
    if clip_count is not None and not (1 <= clip_count <= 10):
        raise HTTPException(status_code=400, detail="clip_count must be between 1 and 10")
    if min_clip_length is not None and not (5 <= min_clip_length <= 120):
        raise HTTPException(status_code=400, detail="min_clip_length must be between 5 and 120 seconds")
    if max_clip_length is not None and not (10 <= max_clip_length <= 180):
        raise HTTPException(status_code=400, detail="max_clip_length must be between 10 and 180 seconds")
    if (min_clip_length is not None and max_clip_length is not None
            and min_clip_length > max_clip_length):
        raise HTTPException(status_code=400, detail="min_clip_length cannot exceed max_clip_length")

    await _maybe_cleanup()
    user_id   = user["user_id"]
    cache_key = _video_cache_key(url, instructions, user_id, clip_style, caption_style,
                                 clip_count, min_clip_length, max_clip_length)

    # Cache hit — same user, same URL, same styles, clips still alive in storage
    if cache_key in _clip_cache:
        job_id = str(uuid.uuid4())
        jobs[job_id] = {
            "status":     "completed",
            "url":        url,
            "results":    _clip_cache[cache_key],
            "error":      None,
            "created_at": time.time(),
            "user_id":    user_id,
            "cached":     True,
        }
        return {"job_id": job_id, "status": "completed", "cached": True}

    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status":     "queued",
        "url":        url,
        "results":    None,
        "error":      None,
        "created_at": time.time(),
        "user_id":    user_id,
        "cached":     False,
    }
    background_tasks.add_task(
        download_and_process, job_id, url, instructions, user_id, cache_key,
        captions, clip_style, caption_style, clip_count, min_clip_length, max_clip_length
    )
    return {"job_id": job_id, "status": "queued"}


@app.post("/upload")
async def upload_video(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    instructions: Optional[str] = Form(None),
    captions: bool = Form(True),
    clip_style: str = Form("auto"),
    caption_style: str = Form("default"),
    clip_count: Optional[int] = Form(None),
    min_clip_length: Optional[int] = Form(None),
    max_clip_length: Optional[int] = Form(None),
    user: dict = Depends(get_current_user),
):
    if clip_style not in clipper.CLIP_STYLES:
        raise HTTPException(status_code=400, detail=f"Invalid clip_style. Choose from: {list(clipper.CLIP_STYLES.keys())}")
    if caption_style not in clipper.CAPTION_PRESETS:
        raise HTTPException(status_code=400, detail=f"Invalid caption_style. Choose from: {list(clipper.CAPTION_PRESETS.keys())}")
    if clip_count is not None and not (1 <= clip_count <= 10):
        raise HTTPException(status_code=400, detail="clip_count must be between 1 and 10")
    if min_clip_length is not None and not (5 <= min_clip_length <= 120):
        raise HTTPException(status_code=400, detail="min_clip_length must be between 5 and 120 seconds")
    if max_clip_length is not None and not (10 <= max_clip_length <= 180):
        raise HTTPException(status_code=400, detail="max_clip_length must be between 10 and 180 seconds")
    if (min_clip_length is not None and max_clip_length is not None
            and min_clip_length > max_clip_length):
        raise HTTPException(status_code=400, detail="min_clip_length cannot exceed max_clip_length")

    await _maybe_cleanup()
    try:
        user_id   = user["user_id"]
        job_id    = str(uuid.uuid4())
        file_path = os.path.join(UPLOAD_DIR, f"{job_id}_{file.filename}")
        with open(file_path, "wb") as buffer:
            buffer.write(await file.read())

        jobs[job_id] = {
            "status":     "queued",
            "results":    None,
            "error":      None,
            "created_at": time.time(),
            "user_id":    user_id,
        }
        background_tasks.add_task(
            process_video_task, job_id, file_path, instructions, user_id, None,
            captions, None, clip_style, caption_style, clip_count, min_clip_length, max_clip_length
        )
        return {"job_id": job_id, "status": "uploaded"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status/{job_id}")
async def get_status(job_id: str, user: dict = Depends(get_current_user)):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    job = jobs[job_id]
    # Users can only see their own jobs
    if job.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not your job")
    return job


@app.post("/stream-token")
async def create_stream_token(user: dict = Depends(get_current_user)):
    """
    Exchange the bearer JWT for a short-lived one-time SSE token.
    EventSource can't send Authorization headers, and putting the JWT in
    the URL would leak it via logs/history — this token is safe to place
    in a query param: single-use, 5-minute expiry, useless for other routes.
    """
    stream_token = uuid.uuid4().hex
    _stream_tokens[stream_token] = {
        "user_id": user["user_id"],
        "expires": time.time() + STREAM_TOKEN_TTL,
    }
    return {"stream_token": stream_token, "expires_in": STREAM_TOKEN_TTL}


@app.get("/stream/{job_id}")
async def stream_status(job_id: str, request: Request, token: str = ""):
    """
    SSE endpoint for real-time job status updates.
    Auth: one-time stream token from POST /stream-token (query param,
    since EventSource doesn't support headers).
    """
    if not token:
        raise HTTPException(status_code=401, detail="Missing token")
    user_id = _consume_stream_token(token)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid or expired stream token")

    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    if jobs[job_id].get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Not your job")

    queue: asyncio.Queue = asyncio.Queue(maxsize=50)
    _sse_subscribers.setdefault(job_id, []).append(queue)

    async def event_generator():
        try:
            # Send current state immediately
            job = jobs.get(job_id, {})
            yield f"data: {json.dumps({'status': job.get('status', 'queued'), 'detail': 'Connected'})}\n\n"

            # If already done, send result and close
            if job.get("status") in ("completed", "failed"):
                yield f"data: {json.dumps(job)}\n\n"
                return

            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"data: {json.dumps(event)}\n\n"
                    # If terminal state, close stream
                    if event.get("status") in ("completed", "failed"):
                        break
                except asyncio.TimeoutError:
                    # Send heartbeat to keep connection alive
                    yield f": heartbeat\n\n"
        finally:
            # Cleanup subscriber
            subs = _sse_subscribers.get(job_id, [])
            if queue in subs:
                subs.remove(queue)
            if not subs:
                _sse_subscribers.pop(job_id, None)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/my-clips")
async def my_clips(user: dict = Depends(get_current_user)):
    """
    Return every clip the user has in Supabase Storage that hasn't expired yet.
    Reads directly from storage — survives server restarts, always accurate.
    Each clip includes:
      - url / video_url / src  : fresh signed download URL
      - expires_at             : when the file will be auto-deleted
      - expires_in_seconds     : countdown (frontend can show a timer)
      - expires_in_human       : e.g. "5h 23m"
      - size_bytes             : file size
      - job_id                 : which processing job produced it
    """
    loop    = asyncio.get_event_loop()
    user_id = user["user_id"]
    clips   = await loop.run_in_executor(None, get_user_clips, user_id)
    return {
        "total":           len(clips),
        "ttl_hours":       6,
        "clips":           clips,
    }


@app.delete("/clips/{job_id}")
async def delete_clips(job_id: str, user: dict = Depends(get_current_user)):
    """Manually delete a job's clips from Supabase Storage."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    job = jobs[job_id]
    if job.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not your job")

    loop = asyncio.get_event_loop()
    deleted = 0
    from supabase_client import _delete_paths
    paths = [c["storage_path"] for c in (job.get("results") or []) if c.get("storage_path")]
    if paths:
        await loop.run_in_executor(None, _delete_paths, paths)
        deleted = len(paths)

    jobs.pop(job_id, None)
    return {"deleted_clips": deleted}


@app.get("/storage-stats")
async def storage_stats(user: dict = Depends(get_current_user)):
    """How many jobs and clips the current user has in memory."""
    user_id   = user["user_id"]
    user_jobs = [j for j in jobs.values() if j.get("user_id") == user_id]
    total_clips = sum(len(j.get("results") or []) for j in user_jobs)
    return {
        "total_jobs":   len(user_jobs),
        "total_clips":  total_clips,
        "active_jobs":  sum(1 for j in user_jobs if j["status"] not in ("completed", "failed")),
    }


@app.get("/clip-styles")
async def list_clip_styles():
    """Available clip styles the user can choose from."""
    return {"styles": [
        {"id": k, "label": k.replace("_", " ").title(), "description": v}
        for k, v in clipper.CLIP_STYLES.items()
    ]}


@app.get("/caption-styles")
async def list_caption_styles():
    """Available caption animation presets."""
    return {"styles": [
        {"id": k, "label": k.replace("_", " ").title(),
         "anim_type": v.get("anim_type", "none")}
        for k, v in clipper.CAPTION_PRESETS.items()
    ]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)