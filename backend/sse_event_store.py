"""Durable SSE event storage shared by all backend workers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from supabase_client import supabase

TABLE = "sse_events"


def publish(job_id: str, event_data: dict[str, Any]) -> int:
    """Persist one SSE event and return its monotonic database id."""
    response = (
        supabase.table(TABLE)
        .insert({"job_id": job_id, "event_data": event_data})
        .select("id")
        .execute()
    )
    if not response.data:
        raise RuntimeError("SSE event insert returned no row")
    return int(response.data[0]["id"])


def events_after(job_id: str, last_event_id: int, limit: int = 100) -> list[dict]:
    """Return events for a job with ids newer than ``last_event_id``."""
    response = (
        supabase.table(TABLE)
        .select("id,event_data")
        .eq("job_id", job_id)
        .gt("id", last_event_id)
        .order("id")
        .limit(limit)
        .execute()
    )
    return response.data or []


def latest_event_id(job_id: str) -> int:
    """Return the latest event id for a job, or zero when none exist."""
    response = (
        supabase.table(TABLE)
        .select("id")
        .eq("job_id", job_id)
        .order("id", desc=True)
        .limit(1)
        .execute()
    )
    if not response.data:
        return 0
    return int(response.data[0]["id"])


def cleanup_older_than(max_age_seconds: int) -> int:
    """Delete SSE events older than ``max_age_seconds`` and return the count."""
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=max_age_seconds)
    response = (
        supabase.table(TABLE)
        .delete()
        .lt("created_at", cutoff.isoformat())
        .select("id")
        .execute()
    )
    return len(response.data or [])
