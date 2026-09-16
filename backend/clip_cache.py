"""Shared clip-result cache backed by Supabase."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from supabase_client import supabase

CACHE_TTL_SECONDS = int(os.getenv("CLIP_TTL_SECONDS", 21600))
TABLE = "clip_cache"


def get(cache_key: str, user_id: str) -> list[dict[str, Any]] | None:
    """Return a non-expired cached result for one user, if present."""
    response = (
        supabase.table(TABLE)
        .select("results,expires_at")
        .eq("cache_key", cache_key)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    )

    if not response.data:
        return None

    row = response.data[0]
    expires_at = _parse_timestamp(row.get("expires_at"))
    if expires_at <= datetime.now(timezone.utc):
        _delete(cache_key, user_id)
        return None

    results = row.get("results")
    return results if isinstance(results, list) else None


def put(cache_key: str, user_id: str, results: list[dict[str, Any]]) -> None:
    """Store or replace a user's cached clip results."""
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=CACHE_TTL_SECONDS)
    (
        supabase.table(TABLE)
        .upsert(
            {
                "cache_key": cache_key,
                "user_id": user_id,
                "results": results,
                "expires_at": expires_at.isoformat(),
            },
            on_conflict="cache_key",
        )
        .execute()
    )


def delete_expired() -> int:
    """Delete expired cache rows and return the number removed."""
    cutoff = datetime.now(timezone.utc).isoformat()
    response = (
        supabase.table(TABLE)
        .delete()
        .lte("expires_at", cutoff)
        .execute()
    )
    return len(response.data or [])


def _delete(cache_key: str, user_id: str) -> None:
    """Delete one cache entry after it has expired."""
    (
        supabase.table(TABLE)
        .delete()
        .eq("cache_key", cache_key)
        .eq("user_id", user_id)
        .execute()
    )


def _parse_timestamp(value: str | None) -> datetime:
    """Parse an ISO timestamp returned by Supabase."""
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return datetime.min.replace(tzinfo=timezone.utc)
