"""Durable SSE event and stream-token storage shared by all backend workers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import secrets
from typing import Any

from supabase_client import supabase

EVENTS_TABLE = "sse_events"
TOKENS_TABLE = "sse_stream_tokens"
TABLE = EVENTS_TABLE


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def publish(job_id: str, user_id: str, event_data: dict[str, Any]) -> int:
    """Persist one SSE event and return its monotonic database id."""
    try:
        response = (
            supabase.table(EVENTS_TABLE)
            .insert(
                {
                    "job_id": job_id,
                    "user_id": user_id,
                    "event_data": event_data,
                }
            )
            .select("id")
            .execute()
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to insert into {EVENTS_TABLE} for job {job_id}: {exc}"
        ) from exc

    if not response.data:
        raise RuntimeError(
            f"Failed to insert into {EVENTS_TABLE} for job {job_id}: "
            "insert returned no row"
        )
    return int(response.data[0]["id"])


def events_after(
    job_id: str,
    user_id: str,
    last_event_id: int,
    limit: int = 100,
) -> list[dict]:
    """Return only newer events owned by the authenticated user."""
    response = (
        supabase.table(EVENTS_TABLE)
        .select("id,event_data")
        .eq("job_id", job_id)
        .eq("user_id", user_id)
        .gt("id", last_event_id)
        .order("id")
        .limit(limit)
        .execute()
    )
    return response.data or []


def latest_event(job_id: str, user_id: str) -> dict | None:
    """Return the latest event for a job owned by the given user."""
    response = (
        supabase.table(EVENTS_TABLE)
        .select("id,event_data")
        .eq("job_id", job_id)
        .eq("user_id", user_id)
        .order("id", desc=True)
        .limit(1)
        .execute()
    )
    if not response.data:
        return None
    return response.data[0]


def latest_event_id(job_id: str, user_id: str) -> int:
    """Return the latest event id for a user-owned job, or zero."""
    event = latest_event(job_id, user_id)
    return int(event["id"]) if event else 0


def cleanup_older_than(max_age_seconds: int) -> int:
    """Delete SSE events older than max_age_seconds and return the count."""
    cutoff = _utcnow() - timedelta(seconds=max_age_seconds)
    response = (
        supabase.table(EVENTS_TABLE)
        .delete()
        .lt("created_at", cutoff.isoformat())
        .select("id")
        .execute()
    )
    return len(response.data or [])


def issue_stream_token(user_id: str, ttl_seconds: int) -> str:
    """Create a random one-time token and persist only its hash."""
    token = secrets.token_urlsafe(32)
    expires_at = _utcnow() + timedelta(seconds=ttl_seconds)
    supabase.table(TOKENS_TABLE).insert(
        {
            "token_hash": _hash_token(token),
            "user_id": user_id,
            "expires_at": expires_at.isoformat(),
        }
    ).execute()
    return token


def consume_stream_token(token: str) -> str | None:
    """
    Atomically consume a one-time stream token.

    The conditional DELETE makes the token single-use across all workers:
    concurrent consumers race on the same database row and only one receives
    the stored user id.
    """
    now = _utcnow()
    response = (
        supabase.table(TOKENS_TABLE)
        .delete()
        .eq("token_hash", _hash_token(token))
        .gt("expires_at", now.isoformat())
        .select("user_id")
        .execute()
    )
    if not response.data:
        return None
    return response.data[0]["user_id"]


def cleanup_expired_stream_tokens() -> int:
    """Delete expired stream tokens and return the number removed."""
    response = (
        supabase.table(TOKENS_TABLE)
        .delete()
        .lt("expires_at", _utcnow().isoformat())
        .select("token_hash")
        .execute()
    )
    return len(response.data or [])
