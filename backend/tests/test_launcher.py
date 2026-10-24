import socket
import time
import types
import unittest
from unittest import mock

from localapp import launcher


class PortSelectionTest(unittest.TestCase):
    def test_occupied_port_moves_to_the_next_free_one(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        self.addCleanup(sock.close)
        busy = sock.getsockname()[1]

        chosen = launcher.pick_port("127.0.0.1", busy, attempts=10)
        self.assertIsNotNone(chosen)
        self.assertNotEqual(chosen, busy)
        self.assertGreater(chosen, busy)
        self.assertTrue(launcher._port_available("127.0.0.1", chosen))

    def test_no_free_port_returns_none(self):
        with mock.patch.object(launcher, "_port_available", return_value=False):
            self.assertIsNone(launcher.pick_port("127.0.0.1", 8756, attempts=3))

    def test_ipv6_url_is_bracketed(self):
        self.assertEqual(launcher.browser_url("::1", 8756),
                         "http://[::1]:8756")


class ServeTest(unittest.TestCase):
    def setUp(self):
        self.module = types.SimpleNamespace(
            app=object(), HOST="127.0.0.1", PORT=8756)

    def test_browser_opens_only_after_startup(self):
        opened = []
        instances = []

        class FakeServer:
            def __init__(self, config):
                self.started = False
                self.should_exit = False
                instances.append(self)

            def run(self):
                time.sleep(0.1)
                self.started = True
                time.sleep(0.15)

        with mock.patch.object(launcher, "pick_port", return_value=9123), \
                mock.patch.object(launcher.uvicorn, "Config",
                                  return_value=object()), \
                mock.patch.object(launcher.uvicorn, "Server", FakeServer):
            launcher.serve(self.module, open_browser=opened.append)

        self.assertEqual(self.module.PORT, 9123)
        self.assertTrue(instances[0].started)
        self.assertEqual(opened, ["http://127.0.0.1:9123"])

    def test_failed_startup_does_not_open_a_browser(self):
        opened = []

        class FailedServer:
            def __init__(self, config):
                self.started = False
                self.should_exit = True

            def run(self):
                pass

        with mock.patch.object(launcher, "pick_port", return_value=9124), \
                mock.patch.object(launcher.uvicorn, "Config",
                                  return_value=object()), \
                mock.patch.object(launcher.uvicorn, "Server", FailedServer):
            launcher.serve(self.module, open_browser=opened.append)
        time.sleep(0.15)
        self.assertEqual(opened, [])

    def test_no_port_exits_before_serving(self):
        with mock.patch.object(launcher, "pick_port", return_value=None), \
                mock.patch.object(launcher, "_report_no_port") as report, \
                mock.patch.object(launcher.uvicorn, "Server") as server:
            with self.assertRaises(SystemExit) as ctx:
                launcher.serve(self.module)
        self.assertEqual(ctx.exception.code, 1)
        report.assert_called_once()
        server.assert_not_called()


if __name__ == "__main__":
    unittest.main()
