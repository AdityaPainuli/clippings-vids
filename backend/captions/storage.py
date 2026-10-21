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
files, and job rows — wired into the main cleanup cadence. Uploads that
never got a job expire the same window after upload.
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
    Signed upload URL so the browser sends the video straight to Supabase
    Storage — the file never passes through this server.
    """
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


def _delete_storage_paths(paths: list) -> bool:
    """Batch delete storage paths. Logs failed batches; False if any failed."""
    if not paths:
        return True
    ok = True
    for i in range(0, len(paths), 100):
        batch = paths[i : i + 100]
        resp = requests.delete(
            f"{_STORAGE_URL}/object/{BUCKET}",
            headers={**_HEADERS, "Content-Type": "application/json"},
            json={"prefixes": batch},
            timeout=_CTRL_TIMEOUT,
        )
        if resp.status_code != 200:
            ok = False
            print(f"[captions cleanup] Storage delete failed {resp.status_code} "
                  f"for {len(batch)} object(s): {resp.text[:200]}")
    return ok


def _list_prefix(prefix: str = "") -> list:
    """List all objects in the bucket under a prefix via paginated REST calls."""
    page_size = 1000
    offset = 0
    items = []

    while True:
        resp = requests.post(
            f"{_STORAGE_URL}/object/list/{BUCKET}",
            headers={**_HEADERS, "Content-Type": "application/json"},
            json={"prefix": prefix, "limit": page_size, "offset": offset},
            timeout=_CTRL_TIMEOUT,
        )
        if resp.status_code != 200:
            print(f"[captions cleanup] Storage list failed {resp.status_code} "
                  f"for {prefix!r}: {resp.text[:200]}")
            return []

        page = resp.json()
        if not isinstance(page, list):
            return []

        items.extend(page)
        if len(page) < page_size:
            return items

        offset += page_size


# ── Retention cleanup ────────────────────────────────────────────────────────

def delete_expired(local_dir: str = "captions_output") -> int:
    """
    Remove storage objects + job rows past expires_at, and any leftover
    local files older than the retention window. Returns rows deleted.
    """
    now = datetime.now(timezone.utc).isoformat()
    res = (supabase.table("caption_jobs")
           .select("id, source_path, output_path")
           .lt("expires_at", now).execute())
    rows = res.data or []

    paths = [p for r in rows for p in (r.get("source_path"), r.get("output_path")) if p]
    # Keep the rows if storage delete failed so the next run retries; once a
    # row is gone nothing points at its output any more.
    deleted = 0
    if rows and _delete_storage_paths(paths):
        supabase.table("caption_jobs").delete().in_("id", [r["id"] for r in rows]).execute()
        deleted = len(rows)

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

    # Last, and contained: a slow or failing storage listing must not stop
    # the local cleanup above or abort the caller's cleanup loop.
    try:
        orphans = _delete_orphaned_sources()
    except requests.RequestException as e:
        print(f"[captions cleanup] orphaned upload sweep failed: {e}")
        orphans = 0

    if deleted:
        print(f"[captions cleanup] {deleted} expired job(s), {len(paths)} storage object(s) removed")
    if orphans:
        print(f"[captions cleanup] {orphans} orphaned upload(s) removed")
    return deleted


def _delete_orphaned_sources() -> int:
    """
    Sources uploaded via a signed URL but never attached to a job have no
    caption_jobs row, so the row-driven pass above never sees them. Scan
    {user_id}/sources/ and drop anything past retention that no remaining job
    references. Runs after expired rows are gone, so any row left is live.
    Returns objects deleted.
    """
    cutoff = time.time() - RETENTION_SECONDS
    candidates = []

    for user_folder in _list_prefix(""):
        user_id = user_folder.get("name", "")
        if not user_id:
            continue
        for job_folder in _list_prefix(f"{user_id}/sources"):
            prefix = f"{user_id}/sources/{job_folder.get('name', '')}"
            for f in _list_prefix(prefix):
                raw_ts = f.get("created_at") or f.get("updated_at", "")
                try:
                    dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    continue
                if dt.timestamp() < cutoff:
                    candidates.append(f"{prefix}/{f['name']}")

    if not candidates:
        return 0

    referenced = set()
    for i in range(0, len(candidates), 100):
        res = (supabase.table("caption_jobs").select("source_path")
               .in_("source_path", candidates[i : i + 100]).execute())
        referenced.update(r["source_path"] for r in res.data or [])

    orphans = [p for p in candidates if p not in referenced]
    if not orphans or not _delete_storage_paths(orphans):
        return 0
    return len(orphans)
