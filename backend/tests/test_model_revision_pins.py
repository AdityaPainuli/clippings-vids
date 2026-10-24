import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from localapp import assets


class ModelRevisionPinTest(unittest.TestCase):
    def test_every_installable_model_has_a_commit_pin(self):
        allowed = set(assets.MODEL_CHOICES) | assets._HIDDEN_MODELS
        self.assertEqual(allowed, set(assets.MODEL_REVISIONS))
        for model, revision in assets.MODEL_REVISIONS.items():
            with self.subTest(model=model):
                self.assertRegex(revision, r"^[0-9a-f]{40}$")

    def test_download_passes_the_pin(self):
        calls = []

        def download_model(name, cache_dir=None, revision=None):
            calls.append((name, cache_dir, revision))

        fake = types.ModuleType("faster_whisper")
        fake.download_model = download_model
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.dict(sys.modules, {"faster_whisper": fake}), \
                mock.patch.object(assets, "MODEL_DIR", Path(tmp)), \
                mock.patch.object(assets, "_model_total_bytes", return_value=1):
            progress = {}
            assets.download_whisper_model("small", progress)
            self.assertTrue((Path(tmp) / ".small.ready").exists())
        self.assertEqual(calls, [("small", str(Path(tmp)), 
                                  assets.MODEL_REVISIONS["small"])])

    def test_size_query_uses_the_same_pin(self):
        seen = {}

        class Sibling:
            size = 123

        class Info:
            siblings = [Sibling()]

        class HfApi:
            def model_info(self, repo, revision=None, files_metadata=False):
                seen.update(repo=repo, revision=revision,
                            files_metadata=files_metadata)
                return Info()

        fw = types.ModuleType("faster_whisper")
        fw.__path__ = []
        utils = types.ModuleType("faster_whisper.utils")
        utils._MODELS = {"small": "Systran/faster-whisper-small"}
        hub = types.ModuleType("huggingface_hub")
        hub.HfApi = HfApi
        with mock.patch.dict(sys.modules, {
                "faster_whisper": fw,
                "faster_whisper.utils": utils,
                "huggingface_hub": hub}):
            total = assets._model_total_bytes("small")
        self.assertEqual(total, 123)
        self.assertEqual(seen["repo"], "Systran/faster-whisper-small")
        self.assertEqual(seen["revision"], assets.MODEL_REVISIONS["small"])
        self.assertTrue(seen["files_metadata"])

    def test_missing_pin_refuses_to_download(self):
        called = []
        fake = types.ModuleType("faster_whisper")
        fake.download_model = lambda *a, **k: called.append(True)
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.dict(sys.modules, {"faster_whisper": fake}), \
                mock.patch.object(assets, "MODEL_DIR", Path(tmp)), \
                mock.patch.object(assets, "_model_total_bytes", return_value=1), \
                mock.patch.dict(assets.MODEL_REVISIONS, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "No pinned revision"):
                assets.download_whisper_model("tiny", {})
        self.assertEqual(called, [])


if __name__ == "__main__":
    unittest.main()
