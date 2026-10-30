
import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from datetime import datetime, timedelta, timezone


class CaptionStorageRetentionTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        client_module = types.ModuleType("supabase_client")
        client_module.supabase = Mock()
        client_module.SUPABASE_URL = "https://example.invalid"
        client_module.SUPABASE_SERVICE_KEY = "test-key"

        self.client_patch = patch.dict(
            sys.modules, {"supabase_client": client_module}
        )
        self.client_patch.start()
        self.addCleanup(self.client_patch.stop)

        import captions

        self.captions = captions
        sys.modules.pop("captions.storage", None)
        if hasattr(captions, "storage"):
            delattr(captions, "storage")

        storage_path = (
            Path(__file__).resolve().parents[1]
            / "captions"
            / "storage.py"
        )
        spec = importlib.util.spec_from_file_location(
            "captions.storage", storage_path
        )
        assert spec is not None and spec.loader is not None

        self.storage = importlib.util.module_from_spec(spec)
        sys.modules["captions.storage"] = self.storage
        spec.loader.exec_module(self.storage)
        captions.storage = self.storage

        self.addCleanup(self._cleanup_storage_module)
        self.storage._delete_storage_paths = Mock()
        self.storage._delete_orphaned_sources = Mock(return_value=0)

    def _cleanup_storage_module(self):
        sys.modules.pop("captions.storage", None)
        if hasattr(self.captions, "storage"):
            delattr(self.captions, "storage")

    def _configure_queries(self, expired_jobs, live_jobs):
        expired_query = Mock()
        expired_query.select.return_value = expired_query
        expired_query.lt.return_value = expired_query
        expired_query.execute.return_value = Mock(data=expired_jobs)

        active_query = Mock()
        active_query.select.return_value = active_query

        active_filter = {
            "column": None,
            "values": None,
            "expires_column": None,
            "expires_value": None,
        }

        def filter_active_sources(column, values):
            active_filter["column"] = column
            active_filter["values"] = list(values)
            return active_query

        def filter_expiry(column, value):
            active_filter["expires_column"] = column
            active_filter["expires_value"] = value
            return active_query

        active_query.in_.side_effect = filter_active_sources
        active_query.gte.side_effect = filter_expiry

        def execute_active_query():
            # Fail if either required query filter is omitted.
            assert active_filter["values"] is not None
            assert active_filter["expires_column"] == "expires_at"
            assert active_filter["expires_value"] is not None

            live_page = [
                job for job in live_jobs
                if job.get(active_filter["column"])
                in active_filter["values"]
                and job.get("expires_at", "")
                >= active_filter["expires_value"]
            ]
            return Mock(data=live_page)

        active_query.execute.side_effect = execute_active_query

        delete_query = Mock()
        delete_query.in_.return_value = delete_query
        delete_query.execute.return_value = Mock(data=[])

        delete_builder = Mock()
        delete_builder.delete.return_value = delete_query

        table = Mock()
        table.side_effect = [
            expired_query,
            active_query,
            delete_builder,
        ]
        self.storage.supabase.table = table

        return delete_query


    def test_expired_transcribe_preserves_source_used_by_live_render(self):
        shared_source = "user-1/sources/upload-1/video.mp4"
        expired_output = "user-1/outputs/transcribe-1/transcript.srt"
        live_expiry = (
            datetime.now(timezone.utc) + timedelta(hours=1)
        ).isoformat()
        delete_query = self._configure_queries(
            [
                {
                    "id": "transcribe-1",
                    "source_path": shared_source,
                    "output_path": expired_output,
                }
            ],
            [{"source_path": shared_source, "expires_at": live_expiry}],
        )

        with patch.object(self.storage.os.path, "isdir", return_value=False):
            deleted_count = self.storage.delete_expired(
                local_dir=self.temp_dir.name
            )

        self.assertEqual(deleted_count, 1)
        self.storage._delete_storage_paths.assert_called_once_with(
            [expired_output]
        )
        delete_query.in_.assert_called_once_with("id", ["transcribe-1"])

    def test_expired_source_is_deleted_when_no_live_job_references_it(self):
        expired_source = "user-1/sources/upload-2/video.mp4"

        self._configure_queries(
            [
                {
                    "id": "transcribe-2",
                    "source_path": expired_source,
                    "output_path": None,
                }
            ],
            [],
        )

        with patch.object(self.storage.os.path, "isdir", return_value=False):
            deleted_count = self.storage.delete_expired(
                local_dir=self.temp_dir.name
            )

        self.assertEqual(deleted_count, 1)
        self.storage._delete_storage_paths.assert_called_once_with(
            [expired_source]
        )

    def test_shared_source_is_preserved_beyond_unfiltered_page_limit(self):
        shared_source = "user-1/sources/upload-1/video.mp4"
        expired_output = "user-1/outputs/transcribe-1/transcript.srt"

        live_expiry = (
            datetime.now(timezone.utc) + timedelta(hours=1)
        ).isoformat()

        live_jobs = [
            {
                "source_path": f"user-1/sources/other-{i}/video.mp4",
                "expires_at": live_expiry,
            }
            for i in range(1000)
        ]
        live_jobs.append({
            "source_path": shared_source,
            "expires_at": live_expiry,
        })
        self._configure_queries(
            [
                {
                    "id": "transcribe-1",
                    "source_path": shared_source,
                    "output_path": expired_output,
                }
            ],
            live_jobs,
        )

        with patch.object(self.storage.os.path, "isdir", return_value=False):
            deleted_count = self.storage.delete_expired(
                local_dir=self.temp_dir.name
            )

        self.assertEqual(deleted_count, 1)
        self.storage._delete_storage_paths.assert_called_once_with(
            [expired_output]
        )


if __name__ == "__main__":
    unittest.main()
