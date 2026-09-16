import unittest
from unittest.mock import patch
from datetime import datetime, timedelta, timezone

import stream_token_store


class FakeResponse:
    def __init__(self, data=None):
        self.data = data or []


class FakeQuery:
    def __init__(self, table, database, operation):
        self.table = table
        self.database = database
        self.operation = operation
        self.filters = {}
        self.payload = None

    def delete(self):
        self.operation = "delete"
        return self

    def upsert(self, payload, on_conflict=None):
        self.operation = "upsert"
        self.payload = payload
        return self

    def update(self, payload):
        self.operation = "update"
        self.payload = payload
        return self

    def select(self, columns):
        return self

    def eq(self, key, value):
        self.filters[key] = ("eq", value)
        return self

    def lt(self, key, value):
        self.filters[key] = ("lt", value)
        return self

    def gt(self, key, value):
        self.filters[key] = ("gt", value)
        return self

    def is_(self, key, value):
        self.filters[key] = ("is", value)
        return self

    def execute(self):
        rows = self.database.setdefault(self.table, {})
        if self.operation == "delete":
            expiry = self.filters.get("expires_at")
            if expiry and expiry[0] == "lt":
                cutoff = datetime.fromisoformat(expiry[1])
                for token_hash in list(rows):
                    expires_at = datetime.fromisoformat(rows[token_hash]["expires_at"])
                    if expires_at < cutoff:
                        del rows[token_hash]
            return FakeResponse()

        if self.operation == "upsert":
            rows[self.payload["token_hash"]] = dict(self.payload)
            return FakeResponse([rows[self.payload["token_hash"]]])

        if self.operation == "update":
            token_hash = self.filters.get("token_hash")
            row = rows.get(token_hash[1]) if token_hash and token_hash[0] == "eq" else None
            if row is None:
                return FakeResponse()
            used_filter = self.filters.get("used_at")
            if used_filter == ("is", "null") and row.get("used_at") is not None:
                return FakeResponse()
            expiry_filter = self.filters.get("expires_at")
            if expiry_filter and expiry_filter[0] == "gt":
                expires_at = datetime.fromisoformat(row["expires_at"])
                now = datetime.fromisoformat(expiry_filter[1])
                if expires_at <= now:
                    return FakeResponse()
            row.update(self.payload)
            return FakeResponse([dict(row)])

        raise AssertionError(f"Unsupported operation: {self.operation}")


class FakeSupabase:
    def __init__(self):
        self.database = {}

    def table(self, name):
        return FakeQuery(name, self.database, "select")


class PersistentStreamTokenStoreTests(unittest.TestCase):
    def setUp(self):
        self.supabase = FakeSupabase()
        self.store = stream_token_store.PersistentStreamTokenStore()
        self.supabase_patch = patch.object(stream_token_store, "supabase", self.supabase)
        self.supabase_patch.start()

    def tearDown(self):
        self.supabase_patch.stop()

    def test_tokens_are_stored_as_hashes(self):
        token = "stream-token"
        expires = (datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()

        self.store[token] = {"user_id": "user-1", "expires": expires}

        rows = self.supabase.database[stream_token_store.TABLE]
        self.assertEqual(len(rows), 1)
        stored = next(iter(rows.values()))
        self.assertNotEqual(stored["token_hash"], token)
        self.assertEqual(stored["user_id"], "user-1")
        self.assertIsNone(stored["used_at"])

    def test_pop_consumes_token_once(self):
        token = "single-use"
        expires = (datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()
        self.store[token] = {"user_id": "user-1", "expires": expires}

        first = self.store.pop(token)
        second = self.store.pop(token)

        self.assertEqual(first["user_id"], "user-1")
        self.assertIsNone(second)

    def test_pop_rejects_expired_token(self):
        token = "expired"
        expires = (datetime.now(timezone.utc) - timedelta(seconds=1)).timestamp()
        self.store[token] = {"user_id": "user-1", "expires": expires}

        self.assertIsNone(self.store.pop(token))

    def test_two_store_instances_share_token_state(self):
        token = "cross-worker"
        expires = (datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()
        first_worker = stream_token_store.PersistentStreamTokenStore()
        second_worker = stream_token_store.PersistentStreamTokenStore()

        first_worker[token] = {"user_id": "user-2", "expires": expires}

        consumed = second_worker.pop(token)
        self.assertEqual(consumed["user_id"], "user-2")

    def test_items_is_empty_without_local_token_state(self):
        token = "no-local-state"
        expires = (datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()
        self.store[token] = {"user_id": "user-3", "expires": expires}

        self.assertEqual(list(self.store.items()), [])


if __name__ == "__main__":
    unittest.main()
