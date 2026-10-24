import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from localapp import assets, server


class WorkDirTest(unittest.TestCase):
    def test_default_follows_bolcap_home(self):
        with mock.patch.dict(os.environ, {}, clear=False), \
                mock.patch.object(assets, "BOLCAP_HOME", Path("/tmp/bolcap-home")):
            os.environ.pop("BOLCAP_WORK_DIR", None)
            self.assertEqual(server._work_dir(),
                             Path("/tmp/bolcap-home/work"))

    def test_explicit_work_dir_wins(self):
        with mock.patch.dict(os.environ, {"BOLCAP_WORK_DIR": "/tmp/elsewhere"}):
            self.assertEqual(server._work_dir(), Path("/tmp/elsewhere"))


class SweepTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.work = Path(self.tmp.name)
        self.patch = mock.patch.object(server, "WORK_DIR", self.work)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def _file(self, name, age_days):
        path = self.work / name
        path.write_bytes(b"x")
        stamp = time.time() - age_days * 86400
        os.utime(path, (stamp, stamp))
        return path

    def test_old_files_are_removed_and_fresh_ones_kept(self):
        old = self._file("old_cut.mp4", 4)
        fresh = self._file("fresh_cut.mp4", 1)
        server._sweep_stale_work()
        self.assertFalse(old.exists())
        self.assertTrue(fresh.exists())

    def test_sweeper_repeats_until_stopped(self):
        calls = []
        stop = threading.Event()
        with mock.patch.object(server, "_sweep_stale_work",
                               side_effect=lambda: calls.append(1)):
            thread = threading.Thread(
                target=server._sweep_forever, args=(stop, 0.01), daemon=True)
            thread.start()
            deadline = time.time() + 2
            while len(calls) < 2 and time.time() < deadline:
                time.sleep(0.01)
            stop.set()
            thread.join(timeout=1)
        self.assertGreaterEqual(len(calls), 2)
        self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
