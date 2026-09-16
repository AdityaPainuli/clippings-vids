"""Durable storage and shared-source helpers for the hosted clipper."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import requests

from supabase_client import SUPABASE_SERVICE_KEY, SUPABASE_URL, supabase

ACTIVE_STATUSES = ("downloading", "analyzing", "clipping", "uploading")
CLIP_SOURCE_BUCKET = "clip-sources"
_STORAGE_URL = f"{SUPABASE_URL}/storage/v1"
_HEADERS = {
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "apikey": SUPABASE_SERVICE_KEY,
}
_STORAGE_TIMEOUT = (10, 600)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def create_job(
    *,
    job_id: str,
    user_id: str,
    url: str | None,
    source_path: str | None,
    instructions: str | None,
    captions: bool,
    clip_style: str,
    caption_style: str,
    clip_count: int | None,
    min_clip_length: int | None,
    max_clip_length: int | None,
    cache_key: str | None = None,
) -> dict:
    now = _now().isoformat()
    row = {
        "id": job_id,
        "user_id": user_id,
        "source_url": url,
        "source_path": source_path,
        "instructions": instructions,
        "captions": captions,
        "clip_style": clip_style,
        "caption_style": caption_style,
        "clip_count": clip_count,
        "min_clip_length": min_clip_length,
        "max_clip_length": max_clip_length,
        "cache_key": cache_key,
        "status": "queued",
        "results": None,
        "error": None,
        "warnings": None,
        "failed_clips": None,
        "cached": False,
        "worker_id": None,
        "lease_until": None,
        "created_at": now,
        "updated_at": now,
    }
    response = supabase.table("clip_jobs").insert(row).execute()
    if not response.data:
        raise RuntimeError("clip_jobs insert returned no row")
    return response.data[0]


def get_job(job_id: str) -> dict | None:
    response = supabase.table("clip_jobs").select("*").eq("id", job_id).limit(1).execute()
    return response.data[0] if response.data else None


def list_jobs(user_id: str) -> list[dict]:
    response = (
        supabase.table("clip_jobs")
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .execute()
    )
    return response.data or []


def update_job(job_id: str, **fields) -> dict:
    fields["updated_at"] = _now().isoformat()
    response = supabase.table("clip_jobs").update(fields).eq("id", job_id).execute()
    if not response.data:
        raise RuntimeError(f"clip job {job_id} no longer exists")
    return response.data[0]


def delete_job(job_id: str) -> None:
    supabase.table("clip_jobs").delete().eq("id", job_id).execute()


def requeue_expired_jobs() -> int:
    now = _now().isoformat()
    response = (
        supabase.table("clip_jobs")
        .update({
            "status": "queued",
            "worker_id": None,
            "lease_until": None,
            "updated_at": now,
        })
        .in_("status", list(ACTIVE_STATUSES))
        .lt("lease_until", now)
        .execute()
    )
    return len(response.data or [])


def claim_next_job(worker_id: str, lease_seconds: int) -> dict | None:
    response = (
        supabase.table("clip_jobs")
        .select("*")
        .eq("status", "queued")
        .order("created_at", desc=False)
        .limit(1)
        .execute()
    )
    if not response.data:
        return None

    candidate = response.data[0]
    lease_until = _now() + timedelta(seconds=lease_seconds)
    claimed = (
        supabase.table("clip_jobs")
        .update({
            "status": "downloading",
            "worker_id": worker_id,
            "lease_until": lease_until.isoformat(),
            "updated_at": _now().isoformat(),
        })
        .eq("id", candidate["id"])
        .eq("status", "queued")
        .execute()
    )
    return claimed.data[0] if claimed.data else None


def renew_lease(job_id: str, worker_id: str, lease_seconds: int) -> None:
    lease_until = _now() + timedelta(seconds=lease_seconds)
    (
        supabase.table("clip_jobs")
        .update({
            "lease_until": lease_until.isoformat(),
            "updated_at": _now().isoformat(),
        })
        .eq("id", job_id)
        .eq("worker_id", worker_id)
        .in_("status", list(ACTIVE_STATUSES))
        .execute()
    )


def upload_source(local_path: str, user_id: str, job_id: str, filename: str) -> str:
    safe_filename = os.path.basename(filename).replace("\\", "_").replace("/", "_")
    storage_path = f"{user_id}/{job_id}/{safe_filename[:120]}"
    with open(local_path, "rb") as source:
        response = requests.post(
            f"{_STORAGE_URL}/object/{CLIP_SOURCE_BUCKET}/{storage_path}",
            headers={**_HEADERS, "x-upsert": "true"},
            files={"file": (safe_filename, source, "application/octet-stream")},
            timeout=_STORAGE_TIMEOUT,
        )
    if response.status_code not in (200, 201):
        raise RuntimeError(f"Source upload failed {response.status_code}: {response.text[:300]}")
    return storage_path


def download_source(storage_path: str, local_path: str) -> None:
    with requests.get(
        f"{_STORAGE_URL}/object/{CLIP_SOURCE_BUCKET}/{storage_path}",
        headers=_HEADERS,
        stream=True,
        timeout=_STORAGE_TIMEOUT,
    ) as response:
        if response.status_code != 200:
            raise RuntimeError(
                f"Source download failed {response.status_code}: {response.text[:300]}"
            )
        with open(local_path, "wb") as target:
            for chunk in response.iter_content(chunk_size=1 << 20):
                target.write(chunk)


def delete_source(storage_path: str | None) -> None:
    if not storage_path:
        return
    requests.delete(
        f"{_STORAGE_URL}/object/{CLIP_SOURCE_BUCKET}",
        headers={**_HEADERS, "Content-Type": "application/json"},
        json={"prefixes": [storage_path]},
        timeout=(10, 30),
    )


def clear_previous_results(job_id: str) -> None:
    rows = (
        supabase.table("clip_metadata")
        .select("storage_path")
        .eq("job_id", job_id)
        .execute()
        .data
        or []
    )
    paths = [row["storage_path"] for row in rows if row.get("storage_path")]
    for start in range(0, len(paths), 100):
        batch = paths[start : start + 100]
        if batch:
            requests.delete(
                f"{_STORAGE_URL}/object/clips",
                headers={**_HEADERS, "Content-Type": "application/json"},
                json={"prefixes": batch},
                timeout=(10, 30),
            )
    if rows:
        supabase.table("clip_metadata").delete().eq("job_id", job_id).execute()
