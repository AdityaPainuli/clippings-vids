from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, Dict
import os
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

def _video_cache_key(url: str, instructions: Optional[str], user_id: str) -> str:
    """Cache key scoped per user so different users don't share clips."""
    raw = f"{user_id}||{url}||{instructions or ''}"
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
):
    loop = asyncio.get_event_loop()
    try:
        # ── 1. Analyse ────────────────────────────────────────────────────────
        jobs[job_id]["status"] = "analyzing"
        clips_metadata = await loop.run_in_executor(
            None, clipper.analyze_video, video_path, instructions, info
        )

        # ── 2. Render clips locally ───────────────────────────────────────────
        jobs[job_id]["status"] = "clipping"
        clips = await loop.run_in_executor(
            None, clipper.create_clips, video_path, clips_metadata, OUTPUT_DIR, captions
        )

        # ── 3. Upload each clip to Supabase Storage ───────────────────────────
        jobs[job_id]["status"] = "uploading"
        results = []
        source_url = jobs[job_id].get("url", "")
        for clip in clips:
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
                "url":           signed_url,
                "video_url":     signed_url,
                "src":           signed_url,
                "storage_path":  storage_path,
                "hook":          clip.get("hook", ""),
                "virality_score": clip.get("virality_score", 0),
                "clip_type":     clip.get("clip_type", ""),
            })

            # Delete local file immediately after upload — no longer needed
            try:
                os.remove(local_path)
            except OSError:
                pass

        jobs[job_id]["status"]  = "completed"
        jobs[job_id]["results"] = results

        if cache_key:
            _clip_cache[cache_key] = results

        # Delete source video
        try:
            os.remove(video_path)
        except OSError:
            pass

    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"]  = str(e)
        print(f"[job {job_id}] failed: {e}")


async def download_and_process(
    job_id: str,
    url: str,
    instructions: Optional[str],
    user_id: str,
    cache_key: str,
    captions: bool = True,
):
    loop = asyncio.get_event_loop()
    try:
        jobs[job_id]["status"] = "downloading"
        video_path, info = await loop.run_in_executor(
            None, clipper.download_video, url, UPLOAD_DIR
        )
        jobs[job_id]["video_path"] = video_path
        await process_video_task(
            job_id, video_path, instructions, user_id, info, captions, cache_key
        )
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"]  = str(e)


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
    user: dict = Depends(get_current_user),
):
    await _maybe_cleanup()
    user_id   = user["user_id"]
    cache_key = _video_cache_key(url, instructions, user_id)

    # Cache hit — same user, same URL, clips still alive in storage
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
        download_and_process, job_id, url, instructions, user_id, cache_key, captions
    )
    return {"job_id": job_id, "status": "queued"}


@app.post("/upload")
async def upload_video(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    instructions: Optional[str] = Form(None),
    captions: bool = Form(True),
    user: dict = Depends(get_current_user),
):
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
            process_video_task, job_id, file_path, instructions, user_id, None, captions, None
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)