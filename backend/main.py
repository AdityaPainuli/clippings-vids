from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse
from typing import Optional
import os
import json
import uuid
import asyncio
import time
import hashlib
import socket

import clipper
import clip_job_store
from supabase_client import supabase, upload_clip_to_storage, delete_old_clips, get_signed_url, get_user_clips
from captions.api import router as captions_router


app = FastAPI()
app.include_router(captions_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads"
OUTPUT_DIR = "output"
CLEANUP_INT = int(os.getenv("CLEANUP_INTERVAL", 1800))
WORKER_POLL_INTERVAL = float(os.getenv("CLIP_WORKER_POLL_SECONDS", 2))
WORKER_LEASE_SECONDS = int(os.getenv("CLIP_WORKER_LEASE_SECONDS", 6 * 3600))

for directory in (UPLOAD_DIR, OUTPUT_DIR):
    os.makedirs(directory, exist_ok=True)

_clip_cache: dict[str, list] = {}
_last_cleanup = time.time()

_sse_subscribers: dict[str, list[asyncio.Queue]] = {}
_stream_tokens: dict[str, dict] = {}
STREAM_TOKEN_TTL = 300

_worker_task: Optional[asyncio.Task] = None
WORKER_ID = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


async def _run_blocking(function, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, function, *args)


def _consume_stream_token(token: str) -> Optional[str]:
    now = time.time()
    expired = [t for t, value in _stream_tokens.items() if value["expires"] < now]
    for expired_token in expired:
        _stream_tokens.pop(expired_token, None)
    entry = _stream_tokens.pop(token, None)
    if entry and entry["expires"] >= now:
        return entry["user_id"]
    return None


def _notify_job(job_id: str, event_data: dict):
    for queue in _sse_subscribers.get(job_id, []):
        try:
            queue.put_nowait(event_data)
        except asyncio.QueueFull:
            pass


bearer = HTTPBearer()


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(bearer)) -> dict:
    token = credentials.credentials
    try:
        user = supabase.auth.get_user(token)
        if not user or not user.user:
            raise HTTPException(status_code=401, detail="Invalid or expired token")
        return {"user_id": user.user.id, "email": user.user.email}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=401, detail=f"Auth error: {str(exc)}")


def _video_cache_key(
    url: str,
    instructions: Optional[str],
    user_id: str,
    clip_style: str = "auto",
    caption_style: str = "default",
    clip_count: Optional[int] = None,
    min_clip_length: Optional[int] = None,
    max_clip_length: Optional[int] = None,
) -> str:
    raw = (
        f"{user_id}||{url}||{instructions or ''}||{clip_style}||{caption_style}"
        f"||{clip_count or ''}||{min_clip_length or ''}||{max_clip_length or ''}"
    )
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


async def _maybe_cleanup():
    global _last_cleanup
    if time.time() - _last_cleanup <= CLEANUP_INT:
        return

    _last_cleanup = time.time()
    deleted = 0
    try:
        deleted = await _run_blocking(delete_old_clips)
    except Exception as exc:
        print(f"[cleanup] Clip cleanup failed: {exc}")

    try:
        from captions.storage import delete_expired
        await _run_blocking(delete_expired)
    except Exception as exc:
        print(f"[cleanup] Caption cleanup failed: {exc}")

    try:
        requeued = await _run_blocking(clip_job_store.requeue_expired_jobs)
    except Exception as exc:
        requeued = 0
        print(f"[cleanup] Clip job recovery failed: {exc}")

    if deleted or requeued:
        print(f"[cleanup] {deleted} storage file(s) deleted, {requeued} clip job(s) requeued")


async def _lease_heartbeat(job_id: str, worker_id: str):
    interval = max(1, WORKER_LEASE_SECONDS // 3)
    try:
        while True:
            await asyncio.sleep(interval)
            await _run_blocking(
                clip_job_store.renew_lease,
                job_id,
                worker_id,
                WORKER_LEASE_SECONDS,
            )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print(f"[worker {worker_id}] lease renewal failed for {job_id}: {exc}")


async def _process_video_task(job: dict, video_path: str, info: Optional[dict] = None):
    job_id = job["id"]
    user_id = job["user_id"]
    instructions = job.get("instructions")
    captions = bool(job.get("captions", True))
    cache_key = job.get("cache_key")
    clip_style = job.get("clip_style", "auto")
    caption_style = job.get("caption_style", "default")
    clip_count = job.get("clip_count")
    min_clip_length = job.get("min_clip_length")
    max_clip_length = job.get("max_clip_length")
    source_url = job.get("source_url") or ""
    worker_id = job.get("worker_id") or WORKER_ID

    try:
        await _run_blocking(clip_job_store.clear_previous_results, job_id)

        await _run_blocking(
            clip_job_store.update_job,
            job_id,
            status="analyzing",
        )
        _notify_job(job_id, {
            "status": "analyzing",
            "detail": "Analyzing video for viral moments...",
        })
        clips_metadata = await _run_blocking(
            clipper.analyze_video,
            video_path,
            instructions,
            info,
            clip_style,
            clip_count,
            min_clip_length,
            max_clip_length,
        )

        if not clips_metadata:
            error = "No viral moments found in this video"
            await _run_blocking(
                clip_job_store.update_job,
                job_id,
                status="failed",
                error=error,
                worker_id=None,
                lease_until=None,
            )
            _notify_job(job_id, {"status": "failed", "error": error})
            return

        _notify_job(job_id, {
            "status": "analyzing",
            "detail": f"Found {len(clips_metadata)} potential clips",
        })

        await _run_blocking(
            clip_job_store.update_job,
            job_id,
            status="clipping",
        )
        _notify_job(job_id, {
            "status": "clipping",
            "detail": f"Rendering {len(clips_metadata)} clips...",
            "total_clips": len(clips_metadata),
        })
        clips, render_failures = await _run_blocking(
            clipper.create_clips,
            video_path,
            clips_metadata,
            OUTPUT_DIR,
            captions,
            caption_style,
        )

        await _run_blocking(
            clip_job_store.update_job,
            job_id,
            status="uploading",
        )
        _notify_job(job_id, {
            "status": "uploading",
            "detail": f"Uploading {len(clips)} clips...",
            "rendered": len(clips),
        })
        results = []
        upload_errors = []

        for clip in clips:
            try:
                local_path = os.path.join(OUTPUT_DIR, clip["filename"])
                storage_path = await _run_blocking(
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
                signed_url = await _run_blocking(get_signed_url, storage_path)
                results.append({
                    **clip,
                    "url": signed_url,
                    "video_url": signed_url,
                    "src": signed_url,
                    "storage_path": storage_path,
                    "hook": clip.get("hook", ""),
                    "virality_score": clip.get("virality_score", 0),
                    "clip_type": clip.get("clip_type", ""),
                })
                try:
                    os.remove(local_path)
                except OSError:
                    pass
            except Exception as exc:
                upload_errors.append({"filename": clip["filename"], "error": str(exc)})
                print(f"  [upload] Failed to upload {clip['filename']}: {exc}")

        total_requested = len(clips_metadata)
        all_errors = render_failures + upload_errors

        if results:
            warnings = None
            failed_clips = None
            if all_errors:
                warnings = f"{len(all_errors)} of {total_requested} clips failed"
                failed_clips = all_errors
            await _run_blocking(
                clip_job_store.update_job,
                job_id,
                status="completed",
                results=results,
                warnings=warnings,
                failed_clips=failed_clips,
                cached=False,
                worker_id=None,
                lease_until=None,
            )
            if cache_key:
                _clip_cache[cache_key] = results
            _notify_job(job_id, {
                "status": "completed",
                "results": results,
                "warnings": warnings,
            })
        else:
            error = f"All {total_requested} clips failed to render/upload"
            await _run_blocking(
                clip_job_store.update_job,
                job_id,
                status="failed",
                error=error,
                failed_clips=all_errors,
                worker_id=None,
                lease_until=None,
            )
            _notify_job(job_id, {"status": "failed", "error": error})
    except Exception as exc:
        try:
            await _run_blocking(
                clip_job_store.update_job,
                job_id,
                status="failed",
                error=str(exc),
                worker_id=None,
                lease_until=None,
            )
        except Exception as update_exc:
            print(f"[worker {worker_id}] could not persist failure for {job_id}: {update_exc}")
        _notify_job(job_id, {"status": "failed", "error": str(exc)})
        print(f"[worker {worker_id}] job {job_id} failed: {exc}")
    finally:
        try:
            os.remove(video_path)
        except OSError:
            pass
        if job.get("source_path"):
            try:
                await _run_blocking(clip_job_store.delete_source, job["source_path"])
            except Exception as exc:
                print(f"[worker {worker_id}] source cleanup failed for {job_id}: {exc}")


async def _run_claimed_job(job: dict):
    job_id = job["id"]
    worker_id = job.get("worker_id") or WORKER_ID
    heartbeat = asyncio.create_task(_lease_heartbeat(job_id, worker_id))
    video_path = os.path.join(UPLOAD_DIR, job_id, "source")
    try:
        job_dir = os.path.dirname(video_path)
        os.makedirs(job_dir, exist_ok=True)
        if job.get("source_path"):
            await _run_blocking(
                clip_job_store.download_source,
                job["source_path"],
                video_path,
            )
            info = None
        else:
            _, info = await _run_blocking(
                clipper.download_video,
                job["source_url"],
                job_dir,
            )
            downloaded_files = [
                os.path.join(job_dir, name)
                for name in os.listdir(job_dir)
                if os.path.isfile(os.path.join(job_dir, name))
            ]
            if not downloaded_files:
                raise RuntimeError("Video download produced no file")
            video_path = downloaded_files[0]
            await _run_blocking(
                clip_job_store.update_job,
                job_id,
                status="downloading",
            )
        await _process_video_task(job, video_path, info)
    except Exception as exc:
        try:
            await _run_blocking(
                clip_job_store.update_job,
                job_id,
                status="failed",
                error=str(exc),
                worker_id=None,
                lease_until=None,
            )
        except Exception as update_exc:
            print(f"[worker {worker_id}] could not persist failure for {job_id}: {update_exc}")
        _notify_job(job_id, {"status": "failed", "error": str(exc)})
        print(f"[worker {worker_id}] job {job_id} failed before processing: {exc}")
        try:
            if job.get("source_path"):
                await _run_blocking(clip_job_store.delete_source, job["source_path"])
        except Exception:
            pass
        try:
            if os.path.exists(video_path):
                os.remove(video_path)
        except OSError:
            pass
    finally:
        heartbeat.cancel()
        try:
            await heartbeat
        except asyncio.CancelledError:
            pass
        try:
            if os.path.isdir(os.path.dirname(video_path)):
                os.rmdir(os.path.dirname(video_path))
        except OSError:
            pass


async def _clip_worker_loop():
    while True:
        try:
            await _run_blocking(clip_job_store.requeue_expired_jobs)
            job = await _run_blocking(
                clip_job_store.claim_next_job,
                WORKER_ID,
                WORKER_LEASE_SECONDS,
            )
            if job:
                await _run_claimed_job(job)
            else:
                await asyncio.sleep(WORKER_POLL_INTERVAL)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[worker {WORKER_ID}] loop failed: {exc}")
            await asyncio.sleep(WORKER_POLL_INTERVAL)


@app.on_event("startup")
async def start_clip_worker():
    global _worker_task
    if _worker_task is None or _worker_task.done():
        _worker_task = asyncio.create_task(_clip_worker_loop())


@app.on_event("shutdown")
async def stop_clip_worker():
    global _worker_task
    if _worker_task is not None:
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
        _worker_task = None


@app.get("/")
async def root():
    return {"message": "Clipwave API is running"}


@app.post("/auth/signup")
async def signup(email: str = Form(...), password: str = Form(...)):
    try:
        res = supabase.auth.sign_up({"email": email, "password": password})
        return {"message": "Signup successful — check your email to confirm", "user_id": res.user.id}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/auth/login")
async def login(email: str = Form(...), password: str = Form(...)):
    try:
        res = supabase.auth.sign_in_with_password({"email": email, "password": password})
        return {
            "access_token": res.session.access_token,
            "refresh_token": res.session.refresh_token,
            "user_id": res.user.id,
            "email": res.user.email,
        }
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))


@app.post("/auth/refresh")
async def refresh_token(refresh_token: str = Form(...)):
    try:
        res = supabase.auth.refresh_session(refresh_token)
        return {
            "access_token": res.session.access_token,
            "refresh_token": res.session.refresh_token,
        }
    except Exception as exc:
        raise HTTPException(status_code=401, detail=str(exc))


def _validate_clip_options(
    clip_style: str,
    caption_style: str,
    clip_count: Optional[int],
    min_clip_length: Optional[int],
    max_clip_length: Optional[int],
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
    if min_clip_length is not None and max_clip_length is not None and min_clip_length > max_clip_length:
        raise HTTPException(status_code=400, detail="min_clip_length cannot exceed max_clip_length")


@app.post("/process-url")
async def process_url(
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
    _validate_clip_options(
        clip_style, caption_style, clip_count, min_clip_length, max_clip_length
    )
    await _maybe_cleanup()

    user_id = user["user_id"]
    cache_key = _video_cache_key(
        url, instructions, user_id, clip_style, caption_style,
        clip_count, min_clip_length, max_clip_length,
    )

    if cache_key in _clip_cache:
        job_id = str(uuid.uuid4())
        await _run_blocking(
            clip_job_store.create_job,
            job_id=job_id,
            user_id=user_id,
            url=url,
            source_path=None,
            instructions=instructions,
            captions=captions,
            clip_style=clip_style,
            caption_style=caption_style,
            clip_count=clip_count,
            min_clip_length=min_clip_length,
            max_clip_length=max_clip_length,
            cache_key=cache_key,
        )
        await _run_blocking(
            clip_job_store.update_job,
            job_id,
            status="completed",
            results=_clip_cache[cache_key],
            cached=True,
            worker_id=None,
            lease_until=None,
        )
        return {"job_id": job_id, "status": "completed", "cached": True}

    job_id = str(uuid.uuid4())
    await _run_blocking(
        clip_job_store.create_job,
        job_id=job_id,
        user_id=user_id,
        url=url,
        source_path=None,
        instructions=instructions,
        captions=captions,
        clip_style=clip_style,
        caption_style=caption_style,
        clip_count=clip_count,
        min_clip_length=min_clip_length,
        max_clip_length=max_clip_length,
        cache_key=cache_key,
    )
    return {"job_id": job_id, "status": "queued"}


@app.post("/upload")
async def upload_video(
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
    _validate_clip_options(
        clip_style, caption_style, clip_count, min_clip_length, max_clip_length
    )
    await _maybe_cleanup()

    user_id = user["user_id"]
    job_id = str(uuid.uuid4())
    temp_dir = os.path.join(UPLOAD_DIR, job_id)
    local_path = os.path.join(temp_dir, "upload")
    os.makedirs(temp_dir, exist_ok=True)

    try:
        with open(local_path, "wb") as output:
            while chunk := await file.read(1 << 20):
                output.write(chunk)

        source_path = await _run_blocking(
            clip_job_store.upload_source,
            local_path,
            user_id,
            job_id,
            file.filename or "upload",
        )
        await _run_blocking(
            clip_job_store.create_job,
            job_id=job_id,
            user_id=user_id,
            url=None,
            source_path=source_path,
            instructions=instructions,
            captions=captions,
            clip_style=clip_style,
            caption_style=caption_style,
            clip_count=clip_count,
            min_clip_length=min_clip_length,
            max_clip_length=max_clip_length,
        )
        return {"job_id": job_id, "status": "uploaded"}
    except Exception as exc:
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
            if os.path.isdir(temp_dir):
                os.rmdir(temp_dir)
        except OSError:
            pass
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        try:
            os.remove(local_path)
        except OSError:
            pass


@app.get("/status/{job_id}")
async def get_status(job_id: str, user: dict = Depends(get_current_user)):
    job = await _run_blocking(clip_job_store.get_job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not your job")
    return job


@app.post("/stream-token")
async def create_stream_token(user: dict = Depends(get_current_user)):
    stream_token = uuid.uuid4().hex
    _stream_tokens[stream_token] = {
        "user_id": user["user_id"],
        "expires": time.time() + STREAM_TOKEN_TTL,
    }
    return {"stream_token": stream_token, "expires_in": STREAM_TOKEN_TTL}


@app.get("/stream/{job_id}")
async def stream_status(job_id: str, request: Request, token: str = ""):
    if not token:
        raise HTTPException(status_code=401, detail="Missing token")
    user_id = _consume_stream_token(token)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid or expired stream token")

    job = await _run_blocking(clip_job_store.get_job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Not your job")

    queue: asyncio.Queue = asyncio.Queue(maxsize=50)
    _sse_subscribers.setdefault(job_id, []).append(queue)

    async def event_generator():
        try:
            current_job = await _run_blocking(clip_job_store.get_job, job_id) or job
            yield f"data: {json.dumps({'status': current_job.get('status', 'queued'), 'detail': 'Connected'})}\n\n"
            if current_job.get("status") in ("completed", "failed"):
                yield f"data: {json.dumps(current_job)}\n\n"
                return

            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"data: {json.dumps(event)}\n\n"
                    if event.get("status") in ("completed", "failed"):
                        break
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            subscribers = _sse_subscribers.get(job_id, [])
            if queue in subscribers:
                subscribers.remove(queue)
            if not subscribers:
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
    loop = asyncio.get_event_loop()
    clips = await _run_blocking(get_user_clips, user["user_id"])
    return {"total": len(clips), "ttl_hours": 6, "clips": clips}


@app.delete("/clips/{job_id}")
async def delete_clips(job_id: str, user: dict = Depends(get_current_user)):
    job = await _run_blocking(clip_job_store.get_job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not your job")
    if job.get("status") not in ("completed", "failed"):
        raise HTTPException(status_code=409, detail="Job is still running")

    paths = [
        clip["storage_path"]
        for clip in (job.get("results") or [])
        if clip.get("storage_path")
    ]
    if paths:
        from supabase_client import _delete_paths
        await _run_blocking(_delete_paths, paths)
    if job.get("source_path"):
        await _run_blocking(clip_job_store.delete_source, job["source_path"])
    await _run_blocking(clip_job_store.delete_job, job_id)
    return {"deleted_clips": len(paths)}


@app.get("/storage-stats")
async def storage_stats(user: dict = Depends(get_current_user)):
    user_jobs = await _run_blocking(clip_job_store.list_jobs, user["user_id"])
    total_clips = sum(len(job.get("results") or []) for job in user_jobs)
    return {
        "total_jobs": len(user_jobs),
        "total_clips": total_clips,
        "active_jobs": sum(
            1 for job in user_jobs
            if job.get("status") not in ("completed", "failed")
        ),
    }


@app.get("/clip-styles")
async def list_clip_styles():
    return {
        "styles": [
            {"id": key, "label": key.replace("_", " ").title(), "description": value}
            for key, value in clipper.CLIP_STYLES.items()
        ]
    }


@app.get("/caption-styles")
async def list_caption_styles():
    return {
        "styles": [
            {
                "id": key,
                "label": key.replace("_", " ").title(),
                "anim_type": value.get("anim_type", "none"),
            }
            for key, value in clipper.CAPTION_PRESETS.items()
        ]
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
