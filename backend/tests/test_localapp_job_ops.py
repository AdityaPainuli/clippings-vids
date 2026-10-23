import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fastapi import HTTPException
from localapp import server


class JobOperationStatusTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.work = Path(self.tmp.name)
        self.patch = mock.patch.object(server, "WORK_DIR", self.work)
        self.patch.start()
        self.addCleanup(self.patch.stop)

        self.job_id = "job1"
        self.job = {
            "status": "ready",
            "video_path": str(self.work / "input.mp4"),
            "filename": "clip.mp4",
            "outputs": {},
            "video_info": {"width": 1920, "height": 1080,
                           "duration": 10.0, "fps": 30.0},
            "transcript": {"words": []},
            "ops": {
                "transcribe": {"status": "ready", "error": None},
                "retakes": {"status": "finding-retakes", "error": None},
                "render": {"status": "rendering", "error": None},
            },
        }
        server.jobs[self.job_id] = self.job
        self.addCleanup(server.jobs.pop, self.job_id, None)

    def test_retakes_finishing_does_not_end_a_render(self):
        with mock.patch.object(server.retakes, "detect",
                               return_value={"cuts": [], "groups": [],
                                             "status": "none-found"}):
            server._retake_worker(self.job_id, [])
        self.assertEqual(self.job["ops"]["retakes"]["status"], "ready")
        self.assertEqual(self.job["ops"]["render"]["status"], "rendering")
        self.assertEqual(self.job["status"], "ready")

    def test_render_finishing_does_not_end_retakes(self):
        srt = self.work / "out.srt"
        srt.write_text("", encoding="utf-8")
        with mock.patch.object(server.engine, "build_ass", return_value=""), \
                mock.patch.object(server.render, "export_srt",
                                  return_value=str(srt)):
            server._render_worker(self.job_id, "srt", mock.Mock(), "hinglish",
                                  [{"start": 0.0, "end": 1.0, "text": "hi"}])
        self.assertEqual(self.job["ops"]["render"]["status"], "ready")
        self.assertEqual(self.job["ops"]["retakes"]["status"],
                         "finding-retakes")
        self.assertEqual(self.job["status"], "ready")

    def test_failures_are_kept_per_operation(self):
        with mock.patch.object(server.retakes, "detect",
                               side_effect=RuntimeError("provider down")):
            server._retake_worker(self.job_id, [])
        self.assertEqual(self.job["ops"]["retakes"]["status"], "failed")
        self.assertIn("provider down", self.job["ops"]["retakes"]["error"])
        self.assertEqual(self.job["ops"]["render"]["status"], "rendering")
        self.assertNotEqual(self.job["status"], "failed")

    def test_status_payload_exposes_both_operations(self):
        payload = asyncio.run(server.job_status(self.job_id))
        self.assertEqual(payload["ops"]["render"]["status"], "rendering")
        self.assertEqual(payload["ops"]["retakes"]["status"],
                         "finding-retakes")

    def test_retakes_endpoint_refuses_a_second_run(self):
        with mock.patch.object(server.llm, "available", return_value=True):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(server.retakes_endpoint(job_id=self.job_id,
                                                     transcript=None))
        self.assertEqual(ctx.exception.status_code, 409)

    def test_render_endpoint_refuses_a_second_render(self):
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(server.render_endpoint(
                job_id=self.job_id, style_json="{}", export="srt",
                text_key="hinglish", transcript=None, cuts=None, markers=None))
        self.assertEqual(ctx.exception.status_code, 409)


if __name__ == "__main__":
    unittest.main()
