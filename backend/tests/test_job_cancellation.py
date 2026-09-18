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



    @patch("captions.api.storage.is_job_cancelled", side_effect=[False, False, False, False, True])
    @patch("captions.api.render.export_srt")
    @patch("captions.api.engine.build_ass", return_value="[Script Info]")
    async def test_cancelled_render_cleans_generated_files(
        self,
        mock_build_ass,
        mock_export_srt,
        mock_is_cancelled,
    ):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as temp_dir:
            with patch("captions.api.WORK_DIR", temp_dir):
                ass_path = os.path.join(temp_dir, "job-render.ass")
                srt_path = os.path.join(temp_dir, "job-render.srt")

                def create_srt_file(words, out_path, words_per_line, text_key="text"):
                    Path(out_path).write_text("test subtitle", encoding="utf-8")
                    return out_path

                mock_export_srt.side_effect = create_srt_file

                style = MagicMock()
                style.words_per_line = 3

                api._render_task(
                    "job-render",
                    "user-123",
                    "test@example.com",
                    None,
                    [{"start": 0, "end": 1, "text": "hello"}],
                    style,
                    "srt",
                    "text",
                    {"width": 1080, "height": 1920, "duration": 1, "fps": 30},
                )

                self.assertFalse(os.path.exists(ass_path))
                self.assertFalse(os.path.exists(srt_path))
                mock_export_srt.assert_called_once()

if __name__ == "__main__":
    unittest.main()
