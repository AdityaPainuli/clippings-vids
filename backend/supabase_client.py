"""
supabase_client.py — single place for all Supabase interactions.

Bucket layout:
  clips/
    {user_id}/
      {job_id}/
        clip_0_xxxxxxxx.mp4
        clip_1_xxxxxxxx.mp4

Signed URLs expire in 6 hours (CLIP_TTL_SECONDS).
delete_old_clips() scans every file in the bucket and removes anything
whose metadata `created_at` is older than CLIP_TTL_SECONDS — called
automatically every 30 minutes from main.py.
"""

import os
import time
import requests
from datetime import datetime, timezone
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
BUCKET               = "clips"
CLIP_TTL_SECONDS     = int(os.getenv("CLIP_TTL_SECONDS", 21600))
SIGNED_URL_SECONDS   = CLIP_TTL_SECONDS

# Used only for auth operations (login/signup/verify JWT)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# Direct REST headers — bypasses supabase-py storage client and RLS entirely
_HEADERS = {
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "apikey":        SUPABASE_SERVICE_KEY,
}
_STORAGE_URL = f"{SUPABASE_URL}/storage/v1"


def _storage_post(endpoint: str, **kwargs) -> requests.Response:
    """POST to Supabase Storage REST API with service-role auth."""
    return requests.post(f"{_STORAGE_URL}{endpoint}", headers=_HEADERS, **kwargs)


def _storage_delete(endpoint: str, json_body: list) -> requests.Response:
    return requests.delete(
        f"{_STORAGE_URL}{endpoint}",
        headers={**_HEADERS, "Content-Type": "application/json"},
        json={"prefixes": json_body},
    )


def _storage_get(endpoint: str) -> requests.Response:
    return requests.get(f"{_STORAGE_URL}{endpoint}", headers=_HEADERS)


# ─────────────────────────────────────────────────────────────────────────────
# Storage helpers — all via REST, no supabase-py storage client
# ─────────────────────────────────────────────────────────────────────────────

def upload_clip_to_storage(
    local_path: str,
    user_id: str,
    job_id: str,
    description: str = "",
    source_url: str = "",
    start_time: float = 0,
    end_time: float = 0,
    hook: str = "",
    virality_score: int = 0,
    clip_type: str = "",
) -> str:
    """
    Upload file to Supabase Storage, then write metadata to clip_metadata table.
    Returns the storage path.
    """
    filename     = os.path.basename(local_path)
    storage_path = f"{user_id}/{job_id}/{filename}"

    # Check file size before uploading (Supabase free tier: 50MB limit)
    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
    if file_size_mb > 50:
        raise RuntimeError(f"Clip too large ({file_size_mb:.1f}MB > 50MB limit). Try a shorter clip.")

    # ── 1. Upload the file ────────────────────────────────────────────────────
    with open(local_path, "rb") as f:
        resp = requests.post(
            f"{_STORAGE_URL}/object/{BUCKET}/{storage_path}",
            headers={
                "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
                "apikey":        SUPABASE_SERVICE_KEY,
                "x-upsert":      "true",
            },
            files={"file": (filename, f, "video/mp4")},
        )

    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Upload failed {resp.status_code}: {resp.text}")

    # ── 2. Write metadata to DB ───────────────────────────────────────────────
    row = {
        "user_id":       user_id,
        "job_id":        job_id,
        "filename":      filename,
        "storage_path":  storage_path,
        "description":   description[:500],
        "source_url":    source_url[:500],
        "start_time":    round(start_time, 2),
        "end_time":      round(end_time, 2),
    }

    # These columns may not exist in older DB schemas — try with them first,
    # fall back without if the insert fails.
    extra_cols = {}
    if hook:
        extra_cols["hook"] = hook[:300]
    if virality_score:
        extra_cols["virality_score"] = virality_score
    if clip_type:
        extra_cols["clip_type"] = clip_type[:50]

    try:
        supabase.table("clip_metadata").insert({**row, **extra_cols}).execute()
    except Exception:
        # Retry without extra columns if they don't exist in the schema
        supabase.table("clip_metadata").insert(row).execute()

    print(f"  [storage] Uploaded + metadata saved → {storage_path}")
    return storage_path


def get_signed_url(storage_path: str) -> str:
    """Generate a signed URL via REST."""
    resp = requests.post(
        f"{_STORAGE_URL}/object/sign/{BUCKET}/{storage_path}",
        headers={**_HEADERS, "Content-Type": "application/json"},
        json={"expiresIn": SIGNED_URL_SECONDS},
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Signed URL failed {resp.status_code}: {resp.text}")

    data  = resp.json()
    token = data.get("signedURL") or data.get("signedUrl", "")
    # Supabase returns a relative path like /object/sign/... — prepend storage base
    if token.startswith("/"):
        return f"{_STORAGE_URL}{token}"
    return token


def _list_prefix(prefix: str = "") -> list:
    """List objects in the bucket under a prefix via REST."""
    resp = requests.post(
        f"{_STORAGE_URL}/object/list/{BUCKET}",
        headers={**_HEADERS, "Content-Type": "application/json"},
        json={"prefix": prefix, "limit": 1000, "offset": 0},
    )
    if resp.status_code != 200:
        return []
    return resp.json() if isinstance(resp.json(), list) else []


def _delete_paths(paths: list) -> None:
    """Batch delete a list of storage paths via REST."""
    if not paths:
        return
    chunk_size = 100
    for i in range(0, len(paths), chunk_size):
        requests.delete(
            f"{_STORAGE_URL}/object/{BUCKET}",
            headers={**_HEADERS, "Content-Type": "application/json"},
            json={"prefixes": paths[i : i + chunk_size]},
        )


def delete_old_clips() -> int:
    """
    Scan all files in the bucket via REST and delete anything older than
    CLIP_TTL_SECONDS. Returns number of files deleted.
    """
    user_folders = _list_prefix("")
    if not user_folders:
        return 0

    to_delete = []

    for folder in user_folders:
        user_id     = folder.get("name", "")
        job_folders = _list_prefix(user_id)

        for job_folder in job_folders:
            job_id   = job_folder.get("name", "")
            prefix   = f"{user_id}/{job_id}"
            files    = _list_prefix(prefix)

            for f in files:
                raw_ts = f.get("updated_at") or f.get("created_at", "")
                try:
                    dt       = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                    file_age = time.time() - dt.timestamp()
                    if file_age > CLIP_TTL_SECONDS:
                        to_delete.append(f"{prefix}/{f['name']}")
                except (ValueError, AttributeError):
                    pass

    if to_delete:
        _delete_paths(to_delete)
        print(f"[cleanup] Deleted {len(to_delete)} clip(s) older than {CLIP_TTL_SECONDS // 3600}h")

    return len(to_delete)


def delete_user_clips(user_id: str) -> int:
    """Delete ALL clips for a user (e.g. on account deletion)."""
    job_folders = _list_prefix(user_id)
    to_delete   = []

    for job_folder in job_folders:
        prefix = f"{user_id}/{job_folder.get('name', '')}"
        files  = _list_prefix(prefix)
        to_delete.extend(f"{prefix}/{f['name']}" for f in files)

    if to_delete:
        _delete_paths(to_delete)
    return len(to_delete)


def get_user_clips(user_id: str) -> list:
    """
    Fetch clip metadata from DB, then generate fresh signed URLs for each.
    Source of truth is clip_metadata table — survives server restarts.
    Only returns clips that are still within the 6-hour TTL window.
    """
    from datetime import timedelta

    now = datetime.now(timezone.utc)

    # ── 1. Fetch all metadata rows for this user, newest first ───────────────
    rows = (
        supabase.table("clip_metadata")
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .execute()
        .data
    )

    if not rows:
        return []

    clips = []
    for row in rows:
        # Parse created_at to calculate TTL
        try:
            created_at = datetime.fromisoformat(
                row["created_at"].replace("Z", "+00:00")
            )
        except (ValueError, AttributeError):
            created_at = now

        expires_at      = created_at + timedelta(seconds=CLIP_TTL_SECONDS)
        expires_in_secs = int((expires_at - now).total_seconds())

        # Skip rows whose files have already passed the TTL
        if expires_in_secs <= 0:
            continue

        # ── 2. Generate a fresh signed URL ────────────────────────────────────
        storage_path   = row["storage_path"]
        remaining_secs = max(60, expires_in_secs)
        try:
            resp = requests.post(
                f"{_STORAGE_URL}/object/sign/{BUCKET}/{storage_path}",
                headers={**_HEADERS, "Content-Type": "application/json"},
                json={"expiresIn": remaining_secs},
            )
            token      = resp.json().get("signedURL") or resp.json().get("signedUrl", "")
            signed_url = f"{_STORAGE_URL}{token}" if token.startswith("/") else token
        except Exception:
            signed_url = None

        clips.append({
            "filename":           row["filename"],
            "job_id":             row["job_id"],
            "storage_path":       storage_path,
            "url":                signed_url,
            "video_url":          signed_url,
            "src":                signed_url,
            "description":        row.get("description", ""),
            "source_url":         row.get("source_url", ""),
            "start_time":         row.get("start_time", 0),
            "end_time":           row.get("end_time", 0),
            "uploaded_at":        created_at.isoformat(),
            "expires_at":         expires_at.isoformat(),
            "expires_in_seconds": expires_in_secs,
            "expires_in_human":   _human_ttl(expires_in_secs),
        })

    return clips


def _human_ttl(seconds: int) -> str:
    """Turn remaining seconds into a readable string like '5h 23m'."""
    if seconds <= 0:
        return "expired"
    h, remainder = divmod(seconds, 3600)
    m = remainder // 60
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"