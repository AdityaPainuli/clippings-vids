"""
Caption storage + durable jobs — Supabase REST, same conventions as
supabase_client.py.

Bucket layout:
  captions/
    {user_id}/
      sources/{job_id}/{filename}    direct browser uploads (signed URL)
      outputs/{job_id}/{filename}    finished renders

Retention: everything expires CAPTION_RETENTION_SECONDS (default 48h)
after job creation. delete_expired() removes storage objects, local
files, job rows, and unclaimed signed uploads — wired into the main cleanup cadence.
"""

import os
import time
from datetime import datetime, timedelta, timezone

import requests

from supabase_client import supabase, SUPABASE_URL, SUPABASE_SERVICE_KEY

BUCKET            = "captions"
RETENTION_SECONDS = int(os.getenv("CAPTION_RETENTION_SECONDS", 48 * 3600))
UPLOAD_URL_TTL_SECONDS = 6 * 3600
UPLOAD_RETENTION_SECONDS = RETENTION_SECONDS + UPLOAD_URL_TTL_SECONDS

_HEADERS     = {"Authorization": f"Bearer {SUPABASE_SERVICE_KEY}", "apikey": SUPABASE_SERVICE_KEY}
_STORAGE_URL = f"{SUPABASE_URL}/storage/v1"

# (connect, read) timeouts — control-plane calls fail fast, transfers get room
_CTRL_TIMEOUT     = (10, 30)
_TRANSFER_TIMEOUT = (10, 600)


# ── Job rows ─────────────────────────────────────────────────────────────────

def create_job(user_id: str, kind: str, **fields) -> str:
    expires = datetime.now(timezone.utc) + timedelta(seconds=RETENTION_SECONDS)
    row = {
        "user_id": user_id, "kind": kind, "status": "queued",
        "expires_at": expires.isoformat(), **fields,
    }
    res = supabase.table("caption_jobs").insert(row).execute()
    if not res.data:
        raise RuntimeError("caption_jobs insert returned no row — is schema.sql applied?")
    return res.data[0]["id"]


def update_job(job_id: str, **fields):
    fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    supabase.table("caption_jobs").update(fields).eq("id", job_id).execute()


def get_job(job_id: str) -> dict | None:
    res = supabase.table("caption_jobs").select("*").eq("id", job_id).execute()
    return res.data[0] if res.data else None


def list_jobs(user_id: str, limit: int = 50) -> list:
    res = (supabase.table("caption_jobs").select(
               "id, kind, status, export, error, filename, created_at, expires_at, seen")
           .eq("user_id", user_id).order("created_at", desc=True).limit(limit).execute())
    return res.data or []


def unseen_completed(user_id: str) -> list:
    """Finished jobs the user hasn't seen — powers the in-app notification feed."""
    res = (supabase.table("caption_jobs").select(
               "id, kind, status, export, error, filename, created_at, expires_at")
           .eq("user_id", user_id).eq("seen", False)
           .in_("status", ["completed", "failed"])
           .order("created_at", desc=True).execute())
    return res.data or []


def mark_seen(user_id: str, job_ids: list):
    if job_ids:
        (supabase.table("caption_jobs").update({"seen": True})
         .eq("user_id", user_id).in_("id", job_ids).execute())


# ── Direct-to-storage uploads ────────────────────────────────────────────────

def create_signed_upload(user_id: str, job_id: str, filename: str) -> dict:
    """
    Register the upload before issuing its signed URL so abandoned objects
    can be reclaimed even when no caption job is ever created for them.
    """
    path = f"{user_id}/sources/{job_id}/{filename}"
    expires = datetime.now(timezone.utc) + timedelta(seconds=UPLOAD_RETENTION_SECONDS)
    row = {
        "user_id": user_id,
        "storage_path": path,
        "expires_at": expires.isoformat(),
    }
    res = supabase.table("caption_uploads").insert(row).execute()
    if not res.data:
        raise RuntimeError("caption_uploads insert returned no row")

    try:
        resp = requests.post(
            f"{_STORAGE_URL}/object/upload/sign/{BUCKET}/{path}",
            headers={**_HEADERS, "Content-Type": "application/json"},
            json={"expiresIn": UPLOAD_URL_TTL_SECONDS},
            timeout=_CTRL_TIMEOUT,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Signed upload failed {resp.status_code}: {resp.text}")
        rel = resp.json().get("url", "")
        return {"storage_path": path,
                "upload_url": f"{_STORAGE_URL}{rel}" if rel.startswith("/") else rel}
    except Exception:
        supabase.table("caption_uploads").delete().eq("storage_path", path).execute()
        raise


def download_to_file(storage_path: str, local_path: str):
    """Stream a storage object to disk (render workers pull sources this way)."""
    with requests.get(f"{_STORAGE_URL}/object/{BUCKET}/{storage_path}",
                      headers=_HEADERS, stream=True, timeout=_TRANSFER_TIMEOUT) as r:
        if r.status_code != 200:
            raise RuntimeError(f"Storage download failed {r.status_code}: {r.text[:200]}")
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)


def upload_output(local_path: str, user_id: str, job_id: str) -> str:
    filename = os.path.basename(local_path)
    path = f"{user_id}/outputs/{job_id}/{filename}"
    mime = "video/mp4" if filename.endswith(".mp4") else \
           "video/quicktime" if filename.endswith(".mov") else "text/plain"
    with open(local_path, "rb") as f:
        resp = requests.post(
            f"{_STORAGE_URL}/object/{BUCKET}/{path}",
            headers={**_HEADERS, "x-upsert": "true"},
            files={"file": (filename, f, mime)},
            timeout=_TRANSFER_TIMEOUT,
        )
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Output upload failed {resp.status_code}: {resp.text}")
    return path


def signed_download_url(storage_path: str, expires_in: int | None = None) -> str:
    resp = requests.post(
        f"{_STORAGE_URL}/object/sign/{BUCKET}/{storage_path}",
        headers={**_HEADERS, "Content-Type": "application/json"},
        json={"expiresIn": expires_in or RETENTION_SECONDS},
        timeout=_CTRL_TIMEOUT,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Signed URL failed {resp.status_code}: {resp.text}")
    data = resp.json()
    token = data.get("signedURL") or data.get("signedUrl", "")
    return f"{_STORAGE_URL}{token}" if token.startswith("/") else token


def _delete_storage_paths(paths: list):
    if not paths:
        return
    unique_paths = list(dict.fromkeys(paths))
    for i in range(0, len(unique_paths), 100):
        requests.delete(
            f"{_STORAGE_URL}/object/{BUCKET}",
            headers={**_HEADERS, "Content-Type": "application/json"},
            json={"prefixes": unique_paths[i : i + 100]},
            timeout=_CTRL_TIMEOUT,
        )


# ── Retention cleanup ────────────────────────────────────────────────────────

def delete_expired(local_dir: str = "captions_output") -> int:
    """
    Remove storage objects + job rows past expires_at, unclaimed signed-upload
    records, and local files older than the retention window.
    Returns the number of caption jobs deleted.
    """
    now = datetime.now(timezone.utc).isoformat()

    jobs_res = (supabase.table("caption_jobs")
                .select("id, source_path, output_path")
                .lt("expires_at", now).execute())
    jobs = jobs_res.data or []

    uploads_res = (supabase.table("caption_uploads")
                   .select("id, storage_path")
                   .lt("expires_at", now).execute())
    uploads = uploads_res.data or []

    paths = [p for r in jobs for p in (r.get("source_path"), r.get("output_path")) if p]
    paths.extend(r["storage_path"] for r in uploads if r.get("storage_path"))
    _delete_storage_paths(paths)

    if jobs:
        supabase.table("caption_jobs").delete().in_("id", [r["id"] for r in jobs]).execute()
    if uploads:
        supabase.table("caption_uploads").delete().in_("id", [r["id"] for r in uploads]).execute()

    # Local temp/render files past retention
    if os.path.isdir(local_dir):
        cutoff = time.time() - RETENTION_SECONDS
        for fname in os.listdir(local_dir):
            fpath = os.path.join(local_dir, fname)
            try:
                if os.path.isfile(fpath) and os.path.getmtime(fpath) < cutoff:
                    os.remove(fpath)
            except OSError:
                pass

    if jobs or uploads:
        print(f"[captions cleanup] {len(jobs)} expired job(s), {len(uploads)} unclaimed upload(s), {len(set(paths))} storage object(s) removed")
    return len(jobs)
