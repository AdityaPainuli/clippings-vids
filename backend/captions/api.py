"""
Caption service API — durable transcribe → edit → render flow.

Upload path for big files: browser asks /captions/upload-url for a signed
Supabase Storage URL and sends the video straight there — it never passes
through this server. Small files (extracted audio) can still be posted
directly to /captions/transcribe.

Jobs live in the caption_jobs table (see schema.sql): they survive
restarts, power the notification feed, and expire after the retention
window (default 48h) when storage + rows are cleaned up.

POST /captions/upload-url        signed direct-to-storage upload URL
POST /captions/transcribe        audio/video (file or storage_path) → transcript
POST /captions/render            transcript + style → export, emailed when done
GET  /captions/jobs              my jobs
GET  /captions/jobs/{id}         job status (transcript included when done)
GET  /captions/download/{id}     redirect to signed download URL
GET  /captions/notifications     unseen finished jobs (in-app bell)
POST /captions/notifications/seen  mark feed items read
GET  /captions/presets           built-in style presets
GET  /captions/style-schema      JSON schema for the style editor UI
"""

import json
import os
import time
import uuid
from typing import Literal, Optional

from fastapi import (APIRouter, BackgroundTasks, Depends, File, Form,
                     HTTPException, UploadFile)
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import ValidationError

from supabase_client import supabase
from . import engine, notify, render, romanize, storage, styles, transcribe

router = APIRouter(prefix="/captions", tags=["captions"])

WORK_DIR = "captions_output"
os.makedirs(WORK_DIR, exist_ok=True)

# Jobs stuck in an active state longer than this are reported failed
# (server restarted mid-render; BackgroundTasks don't survive restarts).
STALE_SECONDS = int(os.getenv("CAPTION_STALE_SECONDS", 1800))

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
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Auth error: {str(e)}")


def _owned_job(job_id: str, user_id: str) -> dict:
    job = storage.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["user_id"] != user_id:
        raise HTTPException(status_code=403, detail="Not your job")
    return job


def _with_staleness(job: dict) -> dict:
    """
    Flag jobs orphaned by a server restart as failed, persisting the state
    so notifications and later reads see the same durable status.
    """
    if job["status"] in ("queued", "transcribing", "romanizing", "rendering"):
        try:
            from datetime import datetime
            updated = datetime.fromisoformat(job["updated_at"].replace("Z", "+00:00"))
            if time.time() - updated.timestamp() > STALE_SECONDS:
                error = "Job interrupted (server restart) — please retry"
                storage.update_job(job["id"], status="failed", error=error)
                job = {**job, "status": "failed", "error": error}
        except (ValueError, KeyError, AttributeError):
            pass
    return job


# ── Direct-to-storage upload ─────────────────────────────────────────────────

@router.post("/upload-url")
async def upload_url(
    filename: str = Form(...),
    user: dict = Depends(get_current_user),
):
    """Browser uploads the video straight to storage with this URL (PUT)."""
    safe_name = os.path.basename(filename).replace(" ", "_")[:120]
    return storage.create_signed_upload(user["user_id"], uuid.uuid4().hex[:12], safe_name)


# ── Transcription ────────────────────────────────────────────────────────────

def _transcribe_task(job_id: str, local_path: str, language: Optional[str],
                     hinglish: bool, cleanup_local: bool):
    try:
        storage.update_job(job_id, status="transcribing")
        result = transcribe.transcribe_video(local_path, language=language)
        if hinglish:
            storage.update_job(job_id, status="romanizing")
            result["words"] = romanize.romanize_words(result["words"])
        info = render.probe_video(local_path)
        storage.update_job(job_id, status="completed", transcript=result, video_info=info)
    except Exception as e:
        storage.update_job(job_id, status="failed", error=str(e)[:500])
    finally:
        if cleanup_local:
            try:
                os.remove(local_path)
            except OSError:
                pass


@router.post("/transcribe")
async def transcribe_endpoint(
    background_tasks: BackgroundTasks,
    file: Optional[UploadFile] = File(None),      # small files (extracted audio)
    storage_path: Optional[str] = Form(None),      # big files via /upload-url
    language: Optional[str] = Form(None),
    hinglish: bool = Form(True),
    user: dict = Depends(get_current_user),
):
    if file is None and not storage_path:
        raise HTTPException(status_code=400, detail="Provide a file or a storage_path")
    if storage_path and not storage_path.startswith(f"{user['user_id']}/"):
        raise HTTPException(status_code=403, detail="Not your upload")

    job_id = storage.create_job(user["user_id"], "transcribe",
                                source_path=storage_path)

    local_path = os.path.join(WORK_DIR, f"{job_id}_source")
    if file is not None:
        # Stream to disk in chunks — never buffer the whole upload in memory
        with open(local_path, "wb") as f:
            while chunk := await file.read(1 << 20):
                f.write(chunk)
    else:
        try:
            storage.download_to_file(storage_path, local_path)
        except Exception as e:
            storage.update_job(job_id, status="failed", error=str(e)[:500])
            raise HTTPException(status_code=502, detail=f"Could not fetch upload: {e}")

    background_tasks.add_task(_transcribe_task, job_id, local_path, language,
                              hinglish, cleanup_local=True)
    return {"job_id": job_id, "status": "queued"}


# ── Styles ───────────────────────────────────────────────────────────────────

@router.get("/presets")
async def list_presets():
    return {"presets": {name: s.model_dump() for name, s in styles.STYLE_PRESETS.items()}}


@router.get("/style-schema")
async def style_schema():
    return styles.CaptionStyle.model_json_schema()


# ── Rendering ────────────────────────────────────────────────────────────────

ExportFormat = Literal["burned", "overlay", "ass", "srt"]


def _render_task(job_id: str, user_id: str, email: str, source_path: Optional[str],
                 words: list, style: styles.CaptionStyle, export: str,
                 text_key: str, video_info: Optional[dict]):
    local_source = None
    try:
        storage.update_job(job_id, status="rendering")

        if source_path:
            local_source = os.path.join(WORK_DIR, f"{job_id}_source")
            storage.download_to_file(source_path, local_source)

        info = video_info or (render.probe_video(local_source) if local_source else
                              {"width": 1080, "height": 1920,
                               "duration": words[-1]["end"], "fps": 30})

        ass_path = os.path.join(WORK_DIR, f"{job_id}.ass")
        with open(ass_path, "w", encoding="utf-8") as f:
            f.write(engine.build_ass(words, style, info["width"], info["height"],
                                     text_key=text_key))

        if export == "ass":
            out = ass_path
        elif export == "srt":
            out = render.export_srt(words, os.path.join(WORK_DIR, f"{job_id}.srt"),
                                    style.words_per_line, text_key=text_key)
        elif export == "overlay":
            out = render.render_overlay(
                ass_path, os.path.join(WORK_DIR, f"{job_id}_overlay.mov"),
                info["width"], info["height"], info["duration"], info["fps"])
        else:
            if not local_source:
                raise RuntimeError("burned export requires an uploaded video")
            out = render.burn_video(
                local_source, ass_path, os.path.join(WORK_DIR, f"{job_id}_subtitled.mp4"))

        # Push the finished file to storage; fall back to serving the local
        # copy if the upload fails (e.g. file exceeds the plan's size limit).
        filename = os.path.basename(out)
        download_url = None
        try:
            output_path = storage.upload_output(out, user_id, job_id)
            download_url = storage.signed_download_url(output_path)
            storage.update_job(job_id, status="completed",
                               output_path=output_path, filename=filename)
            os.remove(out)
        except Exception as up_err:
            print(f"  [render] Output upload failed, serving locally: {up_err}")
            storage.update_job(job_id, status="completed", filename=filename)

        notify.notify_completed(email, job_id, filename, download_url)

    except Exception as e:
        storage.update_job(job_id, status="failed", error=str(e)[:500])
        notify.notify_failed(email, job_id, str(e))
    finally:
        for p in (local_source,):
            if p:
                try:
                    os.remove(p)
                except OSError:
                    pass


@router.post("/render")
async def render_endpoint(
    background_tasks: BackgroundTasks,
    transcript: str = Form(...),           # {"words": [{start,end,text,hinglish?}]}
    style_json: str = Form(...),           # CaptionStyle JSON or {"preset": "name", ...overrides}
    export: ExportFormat = Form("burned"),
    text_key: str = Form("hinglish"),
    storage_path: Optional[str] = Form(None),   # source video from /upload-url
    video_info: Optional[str] = Form(None),     # {"width","height","duration","fps"}
    user: dict = Depends(get_current_user),
):
    try:
        words = json.loads(transcript)["words"]
        if not words:
            raise ValueError("empty transcript")
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Bad transcript: {e}")

    try:
        style_data = json.loads(style_json)
        if "preset" in style_data:
            base = styles.STYLE_PRESETS.get(style_data["preset"])
            if base is None:
                raise ValueError(f"unknown preset {style_data['preset']}")
            overrides = {k: v for k, v in style_data.items() if k != "preset"}
            style = styles.CaptionStyle(**{**base.model_dump(), **overrides})
        else:
            style = styles.CaptionStyle(**style_data)
    except (json.JSONDecodeError, ValidationError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Bad style: {e}")

    if export == "burned" and not storage_path:
        raise HTTPException(status_code=400,
                            detail="burned export requires storage_path (see /captions/upload-url)")
    if storage_path and not storage_path.startswith(f"{user['user_id']}/"):
        raise HTTPException(status_code=403, detail="Not your upload")

    try:
        info = json.loads(video_info) if video_info else None
        if info is not None and not isinstance(info, dict):
            raise ValueError("must be a JSON object")
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Bad video_info: {e}")

    job_id = storage.create_job(user["user_id"], "render", export=export,
                                source_path=storage_path)
    background_tasks.add_task(_render_task, job_id, user["user_id"], user["email"],
                              storage_path, words, style, export, text_key, info)
    return {"job_id": job_id, "status": "queued"}


# ── Jobs / downloads / notifications ─────────────────────────────────────────

@router.get("/jobs")
async def my_jobs(user: dict = Depends(get_current_user)):
    jobs = [_with_staleness(j) for j in storage.list_jobs(user["user_id"])]
    return {"jobs": jobs}


@router.get("/jobs/{job_id}")
async def job_status(job_id: str, user: dict = Depends(get_current_user)):
    job = _with_staleness(_owned_job(job_id, user["user_id"]))
    job.pop("source_path", None)
    return job


@router.get("/download/{job_id}")
async def download(job_id: str, user: dict = Depends(get_current_user)):
    job = _owned_job(job_id, user["user_id"])
    if job.get("status") != "completed":
        raise HTTPException(status_code=409, detail=f"Job status: {job.get('status')}")

    if job.get("output_path"):
        return RedirectResponse(storage.signed_download_url(job["output_path"], 3600))

    # Fallback: output stayed local (storage upload failed)
    from fastapi.responses import FileResponse
    for suffix in ("_subtitled.mp4", "_overlay.mov", ".ass", ".srt"):
        local = os.path.join(WORK_DIR, f"{job_id}{suffix}")
        if os.path.exists(local):
            return FileResponse(local, filename=job.get("filename") or os.path.basename(local))
    raise HTTPException(status_code=410, detail="File expired or removed")


@router.get("/notifications")
async def notifications(user: dict = Depends(get_current_user)):
    return {"notifications": storage.unseen_completed(user["user_id"])}


@router.post("/notifications/seen")
async def notifications_seen(
    job_ids: str = Form(...),   # JSON array of job ids
    user: dict = Depends(get_current_user),
):
    try:
        ids = json.loads(job_ids)
        if not isinstance(ids, list):
            raise ValueError
    except (json.JSONDecodeError, ValueError):
        raise HTTPException(status_code=400, detail="job_ids must be a JSON array")
    storage.mark_seen(user["user_id"], ids)
    return {"marked": len(ids)}
