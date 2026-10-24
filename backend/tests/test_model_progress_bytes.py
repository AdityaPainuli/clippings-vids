import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from localapp import assets


class ModelProgressBytesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.cache = Path(self.tmp.name)
        self.patch = mock.patch.object(assets, "MODEL_DIR", self.cache)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def _blob(self, model, name, size):
        repo = assets._model_repo(model).replace("/", "--")
        blobs = self.cache / f"models--{repo}" / "blobs"
        blobs.mkdir(parents=True, exist_ok=True)
        (blobs / name).write_bytes(b"x" * size)
        return blobs / name

    def test_counts_only_the_target_repo(self):
        self._blob("small", "a", 100)
        self._blob("small", "b", 50)
        self._blob("medium", "other", 999)
        self.assertEqual(assets._model_downloaded_bytes("small"), 150)

    def test_snapshot_symlinks_are_not_counted_twice(self):
        blob = self._blob("small", "a", 100)
        snapshots = (self.cache / "models--Systran--faster-whisper-small"
                     / "snapshots" / "deadbeef")
        snapshots.mkdir(parents=True)
        os.symlink(blob, snapshots / "model.bin")
        self.assertEqual(assets._model_downloaded_bytes("small"), 100)

    def test_missing_cache_is_zero(self):
        self.assertEqual(assets._model_downloaded_bytes("large-v3"), 0)

    def test_cache_dir_uses_the_hf_repo_layout(self):
        self.assertEqual(
            assets._model_cache_dir("small").name,
            "models--Systran--faster-whisper-small")


if __name__ == "__main__":
    unittest.main()
