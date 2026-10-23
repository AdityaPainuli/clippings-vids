import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from captions import render, tighten
from localapp import assets


PROBE_JSON = json.dumps({
    "streams": [{"width": 1080, "height": 1920, "r_frame_rate": "30/1"}],
    "format": {"duration": "10.0"},
})


class RenderTimeoutTest(unittest.TestCase):
    def test_probe_has_a_timeout(self):
        done = mock.Mock(returncode=0, stdout=PROBE_JSON, stderr="")
        with mock.patch("captions.render.subprocess.run", return_value=done) as run:
            render.probe_video("clip.mp4")
        self.assertEqual(run.call_args.kwargs["timeout"], render._PROBE_TIMEOUT)

    def test_probe_timeout_is_reported(self):
        expired = subprocess.TimeoutExpired("ffprobe", render._PROBE_TIMEOUT)
        with mock.patch("captions.render.subprocess.run", side_effect=expired):
            with self.assertRaisesRegex(RuntimeError, "ffprobe timed out"):
                render.probe_video("clip.mp4")

    def test_burn_budget_scales_with_duration(self):
        done = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch("captions.render.subprocess.run", return_value=done) as run:
            render.burn_video("clip.mp4", "captions.ass", "out.mp4", duration=1200)
        self.assertEqual(run.call_args.kwargs["timeout"],
                         render._render_timeout(1200))

    def test_overlay_budget_scales_with_duration(self):
        done = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch("captions.render.subprocess.run", return_value=done) as run:
            render.render_overlay("captions.ass", "overlay.mov", 1080, 1920, 900)
        self.assertEqual(run.call_args.kwargs["timeout"],
                         render._render_timeout(900))

    def test_cut_budget_uses_kept_length(self):
        done = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch("captions.render.subprocess.run", return_value=done) as run:
            with mock.patch("captions.render.probe_audio",
                            return_value={"has_audio": False}):
                render.render_cut("clip.mp4", [(0, 5), (10, 15)], "out.mp4")
        self.assertEqual(run.call_args.kwargs["timeout"],
                         render._render_timeout(10))

    def test_render_timeout_has_a_floor_and_a_ceiling(self):
        self.assertEqual(render._render_timeout(1), render._RENDER_FLOOR)
        self.assertEqual(render._render_timeout(10 ** 9), render._RENDER_MAX)
        self.assertEqual(render._render_timeout(float("nan")), render._RENDER_MAX)


class SilenceTimeoutTest(unittest.TestCase):
    def test_silencedetect_timeout_is_reported(self):
        expired = subprocess.TimeoutExpired("ffmpeg", 300)
        with mock.patch("captions.tighten.subprocess.run", side_effect=expired):
            with self.assertRaisesRegex(RuntimeError, "silencedetect timed out"):
                tighten.detect_silences_from_audio(
                    "clip.mp4", tighten.TightenConfig(), duration=10)

    def test_silencedetect_passes_a_timeout(self):
        done = mock.Mock(returncode=0, stdout="", stderr="")
        with mock.patch("captions.tighten.subprocess.run", return_value=done) as run:
            tighten.detect_silences_from_audio(
                "clip.mp4", tighten.TightenConfig(), duration=600)
        self.assertEqual(run.call_args.kwargs["timeout"],
                         tighten._silence_timeout(600))


class AssetTimeoutTest(unittest.TestCase):
    def test_downloaded_binary_check_has_a_timeout(self):
        with tempfile.TemporaryDirectory() as tmp:
            bin_dir = Path(tmp) / "bin"
            with mock.patch.object(assets, "BIN_DIR", bin_dir), \
                    mock.patch.object(assets, "BOLCAP_HOME", Path(tmp)), \
                    mock.patch.object(assets, "_stream_download"), \
                    mock.patch.object(assets, "_extract_binary"), \
                    mock.patch.object(assets, "_platform_key", return_value="test"), \
                    mock.patch.dict(assets.FFMPEG_SOURCES, {
                        "test": [{"url": "https://example.invalid/ffmpeg.zip",
                                  "binaries": ["ffmpeg", "ffprobe"],
                                  "sha256": "deadbeef"}],
                    }), \
                    mock.patch("localapp.assets.platform.system",
                               return_value="Linux"), \
                    mock.patch("localapp.assets.subprocess.run") as run:
                run.return_value = mock.Mock(returncode=0, stdout="", stderr="")
                assets.download_ffmpeg({"step": "ffmpeg"})
        for call in run.call_args_list:
            self.assertEqual(call.kwargs["timeout"], 30)


if __name__ == "__main__":
    unittest.main()
