"""Durable, single-use storage for browser-safe SSE stream tokens."""

from __future__ import annotations

import hashlib
from collections.abc import ItemsView
from datetime import datetime, timezone

from supabase_client import supabase

TABLE = "stream_tokens"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _cleanup_expired() -> None:
    supabase.table(TABLE).delete().lt("expires_at", _now()).execute()


class PersistentStreamTokenStore:
    """Small mapping-compatible facade used by main.py's existing token flow."""

    def __setitem__(self, token: str, value: dict) -> None:
        _cleanup_expired()
        expires = datetime.fromtimestamp(value["expires"], timezone.utc).isoformat()
        row = {
            "token_hash": _hash_token(token),
            "user_id": value["user_id"],
            "expires_at": expires,
            "used_at": None,
        }
        response = (
            supabase.table(TABLE)
            .upsert(row, on_conflict="token_hash")
            .select("token_hash")
            .execute()
        )
        if not response.data:
            raise RuntimeError("stream token insert returned no row")

    def items(self) -> ItemsView:
        # main.py only uses this to opportunistically remove expired entries.
        # Expiry is enforced atomically by pop(), so there is no local state to list.
        return {}.items()

    def pop(self, token: str, default=None):
        now = _now()
        response = (
            supabase.table(TABLE)
            .update({"used_at": now})
            .eq("token_hash", _hash_token(token))
            .is_("used_at", "null")
            .gt("expires_at", now)
            .select("user_id, expires_at")
            .execute()
        )
        if not response.data:
            return default
        row = response.data[0]
        return {
            "user_id": row["user_id"],
            "expires": datetime.fromisoformat(row["expires_at"].replace("Z", "+00:00")).timestamp(),
        }
