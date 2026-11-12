
import json
import math
import os
import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import (
    APIRouter, BackgroundTasks, Depends, File, Form,
    HTTPException, UploadFile
)
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import ValidationError

from supabase_client import supabase
from . import engine, notify, render, romanize, storage, styles, transcribe


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/captions", tags=["captions"])

WORK_DIR = os.getenv("CAPTION_WORK_DIR", "captions_output")
os.makedirs(WORK_DIR, exist_ok=True)

STALE_SECONDS = max(60, int(os.getenv("CAPTION_STALE_SECONDS", "1800")))
MAX_UPLOAD_BYTES = max(
    1,
    int(os.getenv("CAPTION_MAX_UPLOAD_MB", "500"))
) * 1024 * 1024

ExportFormat = Literal["burned", "overlay", "ass", "srt"]

bearer = HTTPBearer(auto_error=True)

ACTIVE_STATUSES = {
    "queued", "transcribing", "romanizing", "rendering"
}


def _safe_storage_path(path: Optional[str], user_id: str) -> bool:
    """Ensure the storage object belongs to the authenticated user."""
    if not path or not isinstance(path, str):
        return False

    normalized = path.replace("\\", "/")
    parts = normalized.split("/")

    return (
        normalized.startswith(f"{user_id}/")
        and all(part not in ("", ".", "..") for part in parts)
    )


def _job_file(job_id: str, suffix: str) -> str:
    return os.path.join(WORK_DIR, f"{job_id}{suffix}")


def _remove_file(path: Optional[str]) -> None:
    if path:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError:
            logger.warning("Could not remove temporary file: %s", path)


def _validate_video_info(info: Optional[dict]) -> Optional[dict]:
    if info is None:
        return None

    required = ("width", "height", "duration", "fps")

    for key in required:
        if key not in info:
            raise ValueError(f"Missing video_info field: {key}")

        value = info[key]

        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"Invalid video_info field: {key}")

        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"Invalid video_info field: {key}")

    return {
        "width": int(info["width"]),
        "height": int(info["height"]),
        "duration": float(info["duration"]),
        "fps": float(info["fps"]),
    }


def _validate_words(words: object) -> list:
    if not isinstance(words, list) or not words:
        raise ValueError("Transcript must contain a non-empty words array")

    validated = []

    for index, word in enumerate(words):
        if not isinstance(word, dict):
            raise ValueError(f"Word {index} must be an object")

        if not all(key in word for key in ("start", "end", "text")):
            raise ValueError(f"Word {index} requires start, end and text")

        start = word["start"]
        end = word["end"]

        if (
            isinstance(start, bool) or isinstance(end, bool)
            or not isinstance(start, (int, float))
            or not isinstance(end, (int, float))
            or not math.isfinite(start)
            or not math.isfinite(end)
            or start < 0
            or end <= start
        ):
            raise ValueError(f"Invalid timestamps in word {index}")

        if not isinstance(word["text"], str):
            raise ValueError(f"Word {index} text must be a string")

        validated.append(word)

    return validated


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer)
) -> dict:
    try:
        response = supabase.auth.get_user(credentials.credentials)
        auth_user = getattr(response, "user", None)

        if not auth_user:
            raise HTTPException(
                status_code=401,
                detail="Invalid or expired token"
            )

        return {
            "user_id": auth_user.id,
            "email": auth_user.email or ""
        }

    except HTTPException:
        raise
    except Exception:
        logger.exception("Caption authentication failed")
        raise HTTPException(
            status_code=401,
            detail="Authentication failed"
        )


def _owned_job(job_id: str, user_id: str) -> dict:
    job = storage.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.get("user_id") != user_id:
        raise HTTPException(status_code=403, detail="Not your job")

    return job


def _with_staleness(job: dict) -> dict:
    if job.get("status") not in ACTIVE_STATUSES:
        return job

    try:
        updated_at = job.get("updated_at")

        if not updated_at:
            return job

        updated = datetime.fromisoformat(
            updated_at.replace("Z", "+00:00")
        )

        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)

        age = time.time() - updated.timestamp()

        if age > STALE_SECONDS:
            error = "Job interrupted (server restart) — please retry"

            storage.update_job(
                job["id"],
                status="failed",
                error=error
            )

            return {
                **job,
                "status": "failed",
                "error": error
            }

    except (ValueError, KeyError, TypeError, AttributeError):
        logger.warning("Could not check staleness for job")

    return job


# -------------------------------------------------------------------
# Direct-to-storage upload
# -------------------------------------------------------------------

@router.post("/upload-url")
async def upload_url(
    filename: str = Form(...),
    user: dict = Depends(get_current_user)
):
    filename = os.path.basename(filename.strip())

    if not filename or filename in (".", ".."):
        raise HTTPException(status_code=400, detail="Invalid filename")

    safe_name = filename.replace(" ", "_")[:120]

    try:
        return storage.create_signed_upload(
            user["user_id"],
            uuid.uuid4().hex[:12],
            safe_name
        )
    except Exception:
        logger.exception("Could not create signed upload URL")
        raise HTTPException(
            status_code=502,
            detail="Could not create upload URL"
        )


# -------------------------------------------------------------------
# Transcription
# -------------------------------------------------------------------

def _transcribe_task(
    job_id: str,
    local_path: str,
    language: Optional[str],
    hinglish: bool,
    cleanup_local: bool,
    source_path: Optional[str] = None
):
    try:
        storage.update_job(job_id, status="transcribing")

        if source_path:
            storage.download_to_file(source_path, local_path)

        if not os.path.isfile(local_path):
            raise FileNotFoundError("Uploaded source file not found")

        result = transcribe.transcribe_video(
            local_path,
            language=language
        )

        if not isinstance(result, dict):
            raise ValueError("Invalid transcription result")

        words = _validate_words(result.get("words"))

        if hinglish:
            storage.update_job(job_id, status="romanizing")
            words = romanize.romanize_words(words)

        result["words"] = words

        info = render.probe_video(local_path)

        storage.update_job(
            job_id,
            status="completed",
            transcript=result,
            video_info=info
        )

    except Exception as exc:
        logger.exception("Transcription failed for job %s", job_id)

        try:
            storage.update_job(
                job_id,
                status="failed",
                error=str(exc)[:500]
            )
        except Exception:
            logger.exception("Could not update failed transcription job")

    finally:
        if cleanup_local:
            _remove_file(local_path)


@router.post("/transcribe")
async def transcribe_endpoint(
    background_tasks: BackgroundTasks,
    file: Optional[UploadFile] = File(None),
    storage_path: Optional[str] = Form(None),
    language: Optional[str] = Form(None),
    hinglish: bool = Form(True),
    user: dict = Depends(get_current_user)
):
    if file is None and not storage_path:
        raise HTTPException(
            status_code=400,
            detail="Provide a file or a storage_path"
        )

    if storage_path and not _safe_storage_path(
        storage_path, user["user_id"]
    ):
        raise HTTPException(status_code=403, detail="Not your upload")

    job_id = uuid.uuid4().hex
    local_path = _job_file(job_id, "_source")

    if file is not None:
        total_bytes = 0

        try:
            with open(local_path, "wb") as output:
                while True:
                    chunk = await file.read(1024 * 1024)

                    if not chunk:
                        break

                    total_bytes += len(chunk)

                    if total_bytes > MAX_UPLOAD_BYTES:
                        raise HTTPException(
                            status_code=413,
                            detail="Uploaded file exceeds size limit"
                        )

                    output.write(chunk)

        except Exception:
            _remove_file(local_path)
            raise

        finally:
            await file.close()

        if total_bytes == 0:
            _remove_file(local_path)
            raise HTTPException(
                status_code=400,
                detail="Uploaded file is empty"
            )

    try:
        storage.create_job(
            user["user_id"],
            "transcribe",
            source_path=storage_path
        )
    except Exception:
        _remove_file(local_path)
        logger.exception("Could not create transcription job")
        raise HTTPException(
            status_code=500,
            detail="Could not create transcription job"
        )

    download_source_path = storage_path if file is None else None

    background_tasks.add_task(
        _transcribe_task,
        job_id,
        local_path,
        language,
        hinglish,
        True,
        download_source_path
    )

    return {"job_id": job_id, "status": "queued"}


# -------------------------------------------------------------------
# Styles
# -------------------------------------------------------------------

@router.get("/presets")
async def list_presets():
    return {
        "presets": {
            name: preset.model_dump()
            for name, preset in styles.STYLE_PRESETS.items()
        }
    }


@router.get("/style-schema")
async def style_schema():
    return styles.CaptionStyle.model_json_schema()


# -------------------------------------------------------------------
# Rendering
# -------------------------------------------------------------------

def _render_task(
    job_id: str,
    user_id: str,
    email: str,
    source_path: Optional[str],
    words: list,
    style: styles.CaptionStyle,
    export: str,
    text_key: str,
    video_info: Optional[dict]
):
    local_source = None
    ass_path = _job_file(job_id, ".ass")
    output_path = None
    uploaded_successfully = False

    try:
        storage.update_job(job_id, status="rendering")

        if source_path:
            local_source = _job_file(job_id, "_source")
            storage.download_to_file(source_path, local_source)

            if not os.path.isfile(local_source):
                raise FileNotFoundError("Source video not found")

        info = video_info

        if info is None:
            if local_source:
                info = render.probe_video(local_source)
            else:
                raise ValueError(
                    "video_info is required when no source video is provided"
                )

        info = _validate_video_info(info)

        with open(ass_path, "w", encoding="utf-8") as output:
            output.write(
                engine.build_ass(
                    words,
                    style,
                    info["width"],
                    info["height"],
                    text_key=text_key
                )
            )

        if export == "ass":
            output_path = ass_path

        elif export == "srt":
            output_path = render.export_srt(
                words,
                _job_file(job_id, ".srt"),
                style.words_per_line,
                text_key=text_key
            )

        elif export == "overlay":
            output_path = render.render_overlay(
                ass_path,
                _job_file(job_id, "_overlay.mov"),
                info["width"],
                info["height"],
                info["duration"],
                info["fps"]
            )

        elif export == "burned":
            if not local_source:
                raise ValueError(
                    "Burned export requires an uploaded source video"
                )

            output_path = render.burn_video(
                local_source,
                ass_path,
                _job_file(job_id, "_subtitled.mp4")
            )

        else:
            raise ValueError("Unsupported export format")

        if not output_path or not os.path.isfile(output_path):
            raise RuntimeError("Renderer did not produce an output file")

        filename = os.path.basename(output_path)
        download_url = None
        stored_output_path = None

        try:
            stored_output_path = storage.upload_output(
                output_path,
                user_id,
                job_id
            )

            download_url = storage.signed_download_url(stored_output_path)
            uploaded_successfully = True

        except Exception:
            logger.exception(
                "Output upload failed for job %s; keeping local file",
                job_id
            )

        if uploaded_successfully:
            storage.update_job(
                job_id,
                status="completed",
                output_path=stored_output_path,
                filename=filename
            )
            _remove_file(output_path)
        else:
            storage.update_job(
                job_id,
                status="completed",
                filename=filename
            )

        # Notification failure must not change a completed job to failed.
        try:
            notify.notify_completed(
                email,
                job_id,
                filename,
                download_url
            )
        except Exception:
            logger.exception(
                "Completion notification failed for job %s",
                job_id
            )

    except Exception as exc:
        logger.exception("Rendering failed for job %s", job_id)

        try:
            storage.update_job(
                job_id,
                status="failed",
                error=str(exc)[:500]
            )
        except Exception:
            logger.exception("Could not update failed render job")

        try:
            notify.notify_failed(email, job_id, str(exc))
        except Exception:
            logger.exception("Failure notification failed")

        # Do not leave incomplete output files behind.
        _remove_file(output_path)

    finally:
        _remove_file(local_source)

        if output_path != ass_path:
            _remove_file(ass_path)


@router.post("/render")
async def render_endpoint(
    background_tasks: BackgroundTasks,
    transcript: str = Form(...),
    style_json: str = Form(...),
    export: ExportFormat = Form("burned"),
    text_key: str = Form("hinglish"),
    storage_path: Optional[str] = Form(None),
    video_info: Optional[str] = Form(None),
    user: dict = Depends(get_current_user)
):
    # Validate transcript.
    try:
        transcript_data = json.loads(transcript)

        if not isinstance(transcript_data, dict):
            raise ValueError("Transcript must be a JSON object")

        words = _validate_words(transcript_data.get("words"))

    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Bad transcript: {exc}"
        )

    # Validate style.
    try:
        style_data = json.loads(style_json)

        if not isinstance(style_data, dict):
            raise ValueError("Style must be a JSON object")

        if "preset" in style_data:
            preset_name = style_data["preset"]
            base = styles.STYLE_PRESETS.get(preset_name)

            if base is None:
                raise ValueError(f"Unknown preset: {preset_name}")

            overrides = {
                key: value
                for key, value in style_data.items()
                if key != "preset"
            }

            style_data = {
                **base.model_dump(),
                **overrides
            }

        style = styles.CaptionStyle(**style_data)

    except (json.JSONDecodeError, ValidationError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Bad style: {exc}"
        )

    if export == "burned" and not storage_path:
        raise HTTPException(
            status_code=400,
            detail="Burned export requires storage_path"
        )

    if storage_path and not _safe_storage_path(
        storage_path, user["user_id"]
    ):
        raise HTTPException(status_code=403, detail="Not your upload")

    try:
        info = json.loads(video_info) if video_info else None
        info = _validate_video_info(info)

    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Bad video_info: {exc}"
        )

    if not storage_path and info is None:
        raise HTTPException(
            status_code=400,
            detail="video_info is required without a source video"
        )

    job_id = uuid.uuid4().hex

    try:
        storage.create_job(
            user["user_id"],
            "render",
            export=export,
            source_path=storage_path
        )
    except Exception:
        logger.exception("Could not create render job")
        raise HTTPException(
            status_code=500,
            detail="Could not create render job"
        )

    background_tasks.add_task(
        _render_task,
        job_id,
        user["user_id"],
        user["email"],
        storage_path,
        words,
        style,
        export,
        text_key,
        info
    )

    return {"job_id": job_id, "status": "queued"}


# -------------------------------------------------------------------
# Jobs
# -------------------------------------------------------------------

@router.get("/jobs")
async def my_jobs(user: dict = Depends(get_current_user)):
    jobs = [
        _with_staleness(job)
        for job in storage.list_jobs(user["user_id"])
    ]

    # Never expose internal storage paths.
    for job in jobs:
        job.pop("source_path", None)

    return {"jobs": jobs}


@router.get("/jobs/{job_id}")
async def job_status(
    job_id: str,
    user: dict = Depends(get_current_user)
):
    job = _with_staleness(
        _owned_job(job_id, user["user_id"])
    )

    job.pop("source_path", None)

    return job


# -------------------------------------------------------------------
# Downloads
# -------------------------------------------------------------------

@router.get("/download/{job_id}")
async def download(
    job_id: str,
    user: dict = Depends(get_current_user)
):
    job = _with_staleness(
        _owned_job(job_id, user["user_id"])
    )

    if job.get("status") != "completed":
        raise HTTPException(
            status_code=409,
            detail=f"Job status: {job.get('status')}"
        )

    if job.get("output_path"):
        try:
            url = storage.signed_download_url(
                job["output_path"],
                3600
            )
            return RedirectResponse(url)
        except Exception:
            logger.exception("Could not create download URL")
            raise HTTPException(
                status_code=502,
                detail="Could not create download URL"
            )

    filename = job.get("filename")

    if filename:
        allowed_suffixes = (
            "_subtitled.mp4",
            "_overlay.mov",
            ".ass",
            ".srt"
        )

        if not any(filename.endswith(suffix) for suffix in allowed_suffixes):
            raise HTTPException(
                status_code=410,
                detail="Invalid or expired output file"
            )

        local_path = _job_file(
            job_id,
            filename[len(job_id):]
            if filename.startswith(job_id)
            else ""
        )

        if os.path.isfile(local_path):
            return FileResponse(
                local_path,
                filename=filename
            )

    raise HTTPException(
        status_code=410,
        detail="File expired or removed"
    )


# -------------------------------------------------------------------
# Notifications
# -------------------------------------------------------------------

@router.get("/notifications")
async def notifications(user: dict = Depends(get_current_user)):
    return {
        "notifications": storage.unseen_completed(user["user_id"])
    }


@router.post("/notifications/seen")
async def notifications_seen(
    job_ids: str = Form(...),
    user: dict = Depends(get_current_user)
):
    try:
        ids = json.loads(job_ids)

        if (
            not isinstance(ids, list)
            or not all(isinstance(item, str) for item in ids)
        ):
            raise ValueError

        if len(ids) > 500:
            raise ValueError

    except (json.JSONDecodeError, ValueError):
        raise HTTPException(
            status_code=400,
            detail="job_ids must be a JSON array of strings"
        )

    # Verify ownership before marking notifications as seen.
    for job_id in ids:
        _owned_job(job_id, user["user_id"])

    storage.mark_seen(user["user_id"], ids)

    return {"marked": len(ids)}

