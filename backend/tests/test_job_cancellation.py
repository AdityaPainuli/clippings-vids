import os
import sys
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Dummy configuration for importing the application in tests.
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "test-key")

with patch("supabase.create_client", return_value=MagicMock()):
    from captions import api


class TestJobCancellation(unittest.IsolatedAsyncioTestCase):

    @patch("captions.api.storage.cancel_job")
    @patch("captions.api.storage.get_job")
    async def test_cancel_active_job(
        self,
        mock_get_job,
        mock_cancel_job,
    ):
        job_id = "job-123"
        user = {
            "user_id": "user-123",
            "email": "test@example.com",
        }

        mock_get_job.side_effect = [
            {
                "id": job_id,
                "user_id": "user-123",
                "status": "rendering",
            },
        ]

        mock_cancel_job.return_value = {
            "id": job_id,
            "user_id": "user-123",
            "status": "cancelled",
        }

        result = await api.cancel_job_endpoint(job_id, user)

        self.assertEqual(result["job_id"], job_id)
        self.assertEqual(result["status"], "cancelled")

        mock_cancel_job.assert_called_once_with(job_id)

    @patch("captions.api.storage.cancel_job")
    @patch("captions.api.storage.get_job")
    async def test_cannot_cancel_completed_job(
        self,
        mock_get_job,
        mock_cancel_job,
    ):
        job_id = "job-456"
        user = {
            "user_id": "user-123",
            "email": "test@example.com",
        }

        mock_get_job.return_value = {
            "id": job_id,
            "user_id": "user-123",
            "status": "completed",
        }

        with self.assertRaises(api.HTTPException) as context:
            await api.cancel_job_endpoint(job_id, user)

        self.assertEqual(context.exception.status_code, 409)
        mock_cancel_job.assert_not_called()

    @patch("captions.api.storage.cancel_job")
    @patch("captions.api.storage.get_job")
    async def test_already_cancelled_job(
        self,
        mock_get_job,
        mock_cancel_job,
    ):
        job_id = "job-789"
        user = {
            "user_id": "user-123",
            "email": "test@example.com",
        }

        mock_get_job.return_value = {
            "id": job_id,
            "user_id": "user-123",
            "status": "cancelled",
        }

        result = await api.cancel_job_endpoint(job_id, user)

        self.assertEqual(result["status"], "cancelled")
        mock_cancel_job.assert_not_called()


if __name__ == "__main__":
    unittest.main()
