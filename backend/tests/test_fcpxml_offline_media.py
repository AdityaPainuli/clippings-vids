import tempfile
import unittest
import xml.dom.minidom
from pathlib import Path
from unittest import mock

from captions import timeline
from localapp import server


KEEP = [(0.0, 5.0), (7.0, 10.0)]


class OfflineFcpxmlTest(unittest.TestCase):
    def test_no_path_leaves_media_for_relinking(self):
        doc = timeline.build_fcpxml(
            KEEP, 30.0, 1920, 1080, None, 12.0, clip_name="take 1.mp4")
        dom = xml.dom.minidom.parseString(doc)
        asset = dom.getElementsByTagName("asset")[0]
        self.assertEqual(asset.getAttribute("name"), "take 1.mp4")
        self.assertEqual(len(dom.getElementsByTagName("media-rep")), 0)
        self.assertNotIn("file://", doc)
        self.assertNotIn("~/.bolcap", doc)

    def test_a_real_path_still_gets_a_media_url(self):
        doc = timeline.build_fcpxml(
            KEEP, 30.0, 1920, 1080, "/tmp/take 1.mp4", 12.0)
        self.assertIn('kind="original-media"', doc)
        self.assertIn("file:///tmp/take%201.mp4", doc)

    def test_desktop_export_does_not_embed_the_work_copy(self):
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            job_id = "job1"
            job = {
                "status": "ready",
                "video_path": str(work / "job1_upload.mp4"),
                "filename": "take 1.mp4",
                "outputs": {},
                "video_info": {"width": 1920, "height": 1080,
                               "duration": 12.0, "fps": 30.0},
                "transcript": {"words": []},
            }
            server.jobs[job_id] = job
            self.addCleanup(server.jobs.pop, job_id, None)
            with mock.patch.object(server, "WORK_DIR", work), \
                    mock.patch.object(server.timeline, "export_fcpxml",
                                      return_value=str(work / "out.fcpxml")) as export, \
                    mock.patch.object(server.render, "probe_audio",
                                      return_value={"has_audio": True,
                                                    "channels": 2,
                                                    "sample_rate": 48000}):
                server._render_worker(
                    job_id, "fcpxml", mock.Mock(), "hinglish",
                    [{"start": 0.0, "end": 1.0, "text": "hi"}],
                    cuts=[{"start": 5.0, "end": 7.0}])
        self.assertIsNone(export.call_args.args[4])
        self.assertEqual(export.call_args.kwargs["clip_name"], "take 1.mp4")
        self.assertEqual(job["status"], "ready")


if __name__ == "__main__":
    unittest.main()
