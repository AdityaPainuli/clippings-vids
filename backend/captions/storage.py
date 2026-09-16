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
files, and job rows — wired into the main cleanup cadence.
"""

import os
import time
from datetime import datetime, timedelta, timezone

import requests

from supabase_client import supabase, SUPABASE_URL, SUPABASE_SERVICE_KEY

BUCKET            = "captions"
RETENTION_SECONDS = int(os.getenv("CAPTION_RETENTION_SECONDS", 48 * 3600))

_HEADERS     = {"Authorization": f"Bearer {SUPABASE_SERVICE_KEY}", "apikey": SUPABASE_SERVICE_KEY}
_STORAGE_URL = f"{SUPABASE_URL}/storage/v1"

# (connect, read) timeouts — control-plane calls fail fast, transfers get room
_CTRL_TIMEOUT     = (10, 30)
_TRANSFER_TIMEOUT = (10, 600)

_ACTIVE_STATUSES = ("queued", "transcribing", "romanizing", "rendering")


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
    """Update a job without allowing normal workers to resurrect a cancelled job."""
    next_status = fields.get("status")
    if next_status not in (None, "cancelled", "failed"):
        current = get_job(job_id)
        if current and current.get("status") == "cancelled":
            return False

    fields["updated_at"] = datetime.now(timezone.utc).isoformat()
    supabase.table("caption_jobs").update(fields).eq("id", job_id).execute()
    return True


def get_job(job_id: str) -> dict | None:
    res = supabase.table("caption_jobs").select("*").eq("id", job_id).execute()
    return res.data[0] if res.data else None


def request_cancel(job_id: str, user_id: str) -> str:
    """Cancel a queued/active job while enforcing ownership and terminal-state rules."""
    job = get_job(job_id)
    if not job:
        return "not_found"
    if job.get("user_id") != user_id:
        return "forbidden"

    status = job.get("status")
    if status == "cancelled":
        return "already_cancelled"
    if status in ("completed", "failed"):
        return status

    response = (supabase.table("caption_jobs")
                .update({
                    "status": "cancelled",
                    "error": "Job cancelled by user",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
                .eq("id", job_id)
                .eq("user_id", user_id)
                .in_("status", list(_ACTIVE_STATUSES))
                .execute())

    if response.data:
        return "cancelled"

    current = get_job(job_id)
    if not current:
        return "not_found"
    return current.get("status", "failed") if current.get("user_id") == user_id else "forbidden"


def is_cancelled(job_id: str) -> bool:
    job = get_job(job_id)
    return bool(job and job.get("status") == "cancelled")


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
           .in_("status", ["completed", "failed", "cancelled"])
           .order("created_at", desc=True).execute())
    return res.data or []


def mark_seen(user_id: str, job_ids: list):
    if job_ids:
        (supabase.table("caption_jobs").update({"seen": True})
         .eq("user_id", user_id).in_("id", job_ids).execute())


# ── Direct-to-storage uploads ────────────────────────────────────────────────

def create_signed_upload(user_id: str, job_id: str, filename: str) -> dict:
    """Signed upload URL so the browser sends the video straight to Supabase Storage."""
    path = f"{user_id}/sources/{job_id}/{filename}"
    resp = requests.post(
        f"{_STORAGE_URL}/object/upload/sign/{BUCKET}/{path}",
        headers={**_HEADERS, "Content-Type": "application/json"},
        json={"expiresIn": 6 * 3600},
        timeout=_CTRL_TIMEOUT,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Signed upload failed {resp.status_code}: {resp.text}")
    rel = resp.json().get("url", "")
    return {"storage_path": path,
            "upload_url": f"{_STORAGE_URL}{rel}" if rel.startswith("/") else rel}


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
    for i in range(0, len(paths), 100):
        requests.delete(
            f"{_STORAGE_URL}/object/{BUCKET}",
            headers={**_HEADERS, "Content-Type": "application/json"},
            json={"prefixes": paths[i : i + 100]},
            timeout=_CTRL_TIMEOUT,
        )


# ── Retention cleanup ────────────────────────────────────────────────────────

def delete_expired(local_dir: str = "captions_output") -> int:
    """Remove storage objects + job rows past expires_at and old local files."""
    now = datetime.now(timezone.utc).isoformat()
    res = (supabase.table("caption_jobs")
           .select("id, source_path, output_path")
           .lt("expires_at", now).execute())
    rows = res.data or []

    paths = [p for r in rows for p in (r.get("source_path"), r.get("output_path")) if p]
    _delete_storage_paths(paths)
    if rows:
        supabase.table("caption_jobs").delete().in_("id", [r["id"] for r in rows]).execute()

    if os.path.isdir(local_dir):
        cutoff = time.time() - RETENTION_SECONDS
        for fname in os.listdir(local_dir):
            fpath = os.path.join(local_dir, fname)
            try:
                if os.path.isfile(fpath) and os.path.getmtime(fpath) < cutoff:
                    os.remove(fpath)
            except OSError:
                pass

    if rows:
        print(f"[captions cleanup] {len(rows)} expired job(s), {len(paths)} storage object(s) removed")
    return len(rows)
