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


class TestStorageListing(unittest.TestCase):
    def setUp(self):
        # Restore real supabase_client if stubbed by another test
        if "supabase_client" in sys.modules and not hasattr(sys.modules["supabase_client"], "_list_prefix"):
            del sys.modules["supabase_client"]
        import supabase_client
        self.supabase_client = supabase_client

    @patch("requests.post")
    def test_successful_empty_listing(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = []
        mock_post.return_value = mock_resp

        res = self.supabase_client._list_prefix("test-prefix")
        self.assertEqual(res, [])

    @patch("requests.post")
    def test_successful_populated_listing(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [{"name": "file1.mp4"}, {"name": "file2.mp4"}]
        mock_post.return_value = mock_resp

        res = self.supabase_client._list_prefix("test-prefix")
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]["name"], "file1.mp4")

    @patch("requests.post")
    def test_http_error_raises_runtime_error(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_post.return_value = mock_resp

        with self.assertRaises(RuntimeError) as ctx:
            self.supabase_client._list_prefix("test-prefix")
        self.assertIn("status 500", str(ctx.exception))

    @patch("requests.post")
    def test_invalid_json_raises_runtime_error(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.side_effect = ValueError("No JSON object could be decoded")
        mock_post.return_value = mock_resp

        with self.assertRaises(RuntimeError) as ctx:
            self.supabase_client._list_prefix("test-prefix")
        self.assertIn("invalid JSON", str(ctx.exception))

    @patch("requests.post")
    def test_unexpected_dict_shape_raises_runtime_error(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"error": "Invalid prefix", "statusCode": "400"}
        mock_post.return_value = mock_resp

        with self.assertRaises(RuntimeError) as ctx:
            self.supabase_client._list_prefix("test-prefix")
        self.assertIn("unexpected response structure", str(ctx.exception))

    def test_cleanup_propagates_listing_error(self):
        with patch.object(self.supabase_client, "_list_prefix", side_effect=RuntimeError("Storage outage")):
            with self.assertRaises(RuntimeError):
                self.supabase_client.delete_old_clips()


if __name__ == "__main__":
    unittest.main()
