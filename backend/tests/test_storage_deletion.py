import os
import sys
import types
import unittest
from unittest.mock import patch, MagicMock

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-key")

if "supabase" not in sys.modules:
    sb_mod = types.ModuleType("supabase")
    sb_mod.create_client = lambda url, key: MagicMock()
    sb_mod.Client = object
    sys.modules["supabase"] = sb_mod

if "supabase_client" in sys.modules and not hasattr(sys.modules["supabase_client"], "SUPABASE_URL"):
    del sys.modules["supabase_client"]


class TestStorageDeletion(unittest.TestCase):
    def setUp(self):
        # Restore real supabase_client if stubbed by another test
        if "supabase_client" in sys.modules and not hasattr(sys.modules["supabase_client"], "SUPABASE_URL"):
            del sys.modules["supabase_client"]

        # Restore real captions package in sys.modules if stubbed
        if "captions" in sys.modules and (not hasattr(sys.modules["captions"], "__file__") or not sys.modules["captions"].__file__):
            del sys.modules["captions"]
            for mod in list(sys.modules.keys()):
                if mod.startswith("captions."):
                    del sys.modules[mod]

        if "captions.storage" in sys.modules and not hasattr(sys.modules["captions.storage"], "_delete_storage_paths"):
            del sys.modules["captions.storage"]

        import captions.storage
        self.storage = captions.storage

    @patch("requests.delete")
    def test_successful_storage_deletion(self, mock_del):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_del.return_value = mock_resp

        # Should complete with no exception
        self.storage._delete_storage_paths(["path/1.mp4", "path/2.mp4"])
        mock_del.assert_called_once()

    @patch("requests.delete")
    def test_failed_storage_deletion_raises_runtime_error(self, mock_del):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_del.return_value = mock_resp

        with self.assertRaises(RuntimeError) as ctx:
            self.storage._delete_storage_paths(["path/1.mp4"])
        self.assertIn("status 500", str(ctx.exception))

    @patch("requests.delete")
    def test_transport_error_propagates(self, mock_del):
        import requests
        mock_del.side_effect = requests.exceptions.ConnectionError("Connection refused")

        with self.assertRaises(requests.exceptions.ConnectionError):
            self.storage._delete_storage_paths(["path/1.mp4"])

    @patch("requests.delete")
    def test_batching_over_100_paths(self, mock_del):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_del.return_value = mock_resp

        paths = [f"path/{i}.mp4" for i in range(250)]
        self.storage._delete_storage_paths(paths)
        self.assertEqual(mock_del.call_count, 3)

    @patch("requests.delete")
    def test_delete_expired_aborts_db_deletion_on_storage_failure(self, mock_del):
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        mock_resp.text = "Service Unavailable"
        mock_del.return_value = mock_resp

        # Mock supabase table caption_jobs select returning expired rows
        mock_table = MagicMock()
        mock_select = MagicMock()
        mock_lt = MagicMock()
        mock_res = MagicMock()
        mock_res.data = [{"id": "job-1", "source_path": "path/1.mp4", "output_path": "path/out1.mp4"}]
        mock_lt.execute.return_value = mock_res
        mock_select.lt.return_value = mock_lt
        mock_table.select.return_value = mock_select

        with patch.object(self.storage.supabase, "table", return_value=mock_table):
            with self.assertRaises(RuntimeError):
                self.storage.delete_expired(local_dir="/non/existent/dir")

            # Crucial invariant: delete() on DB table must NOT have been called
            mock_table.delete.assert_not_called()


if __name__ == "__main__":
    unittest.main()
