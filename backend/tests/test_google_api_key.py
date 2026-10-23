import asyncio
import importlib
import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.extend([BACKEND_DIR, os.path.dirname(os.path.abspath(__file__))])

for mod in ("google", "google.generativeai", "moviepy", "yt_dlp"):
    sys.modules.setdefault(mod, MagicMock())

from test_main_job_race import _install_main_import_stubs


class GoogleApiKeyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_main_import_stubs()
        cls.main = importlib.import_module("main")
        sys.modules.pop("clipper", None)
        cls.clipper = importlib.import_module("clipper")

    def tearDown(self):
        self.clipper.GENAI_API_KEY = None

    def test_missing_key_raises_error(self):
        for key in (None, ""):
            with self.subTest(key=key):
                env = {} if key is None else {"GOOGLE_API_KEY": key}
                with patch.dict(os.environ, env, clear=key is None):
                    importlib.reload(self.clipper)
                    with self.assertRaises(self.clipper.MissingAPIKeyError) as ctx:
                        self.clipper.analyze_video("fake_video.mp4")
                    self.assertIn("GOOGLE_API_KEY", str(ctx.exception))

    def test_configured_key_calls_gemini(self):
        with patch.dict(os.environ, {"GOOGLE_API_KEY": "test-key-abc123"}):
            importlib.reload(self.clipper)
            fake_response = MagicMock(text=json.dumps([{
                "start_time": 5.0, "end_time": 35.0, "description": "Great moment",
                "hook": "Hook text", "virality_score": 8, "clip_type": "insight"
            }]))
            fake_model = MagicMock()
            fake_model.generate_content.return_value = fake_response
            self.clipper.genai.GenerativeModel.return_value = fake_model

            with patch.object(self.clipper, "_extract_keyframes",
                              return_value=([{"timestamp": 5.0, "data": "aGVsbG8="}], 60.0)), \
                 patch.object(self.clipper, "_find_vtt_file", return_value=None):
                result = self.clipper.analyze_video("fake_video.mp4", info={})

            fake_model.generate_content.assert_called_once()
            self.assertIsInstance(result, list)
            self.assertGreater(len(result), 0)
            for clip in result:
                self.assertNotEqual(clip.get("description"), "Short clip (API Key missing)")

    def test_job_fails_without_results(self):
        job_id = "test-job-001"
        self.main.jobs[job_id] = {
            "status": "queued",
            "url": "https://youtube.com/watch?v=test",
            "results": None,
            "error": None,
        }
        with patch.object(self.main.clipper, "analyze_video",
                          side_effect=self.clipper.MissingAPIKeyError("GOOGLE_API_KEY is not configured.")):
            asyncio.run(self.main.process_video_task(
                job_id=job_id,
                video_path="fake.mp4",
                instructions=None,
                user_id="user-abc",
            ))
        job = self.main.jobs[job_id]
        self.assertEqual(job["status"], "failed")
        self.assertIn("GOOGLE_API_KEY", job.get("error", ""))
        self.assertFalse(bool(job.get("results")))


if __name__ == "__main__":
    unittest.main()
