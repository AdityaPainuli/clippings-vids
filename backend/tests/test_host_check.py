import os
import sys
import unittest

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

from localapp.hostcheck import allowed_hosts, host_allowed  # noqa: E402


class HostCheckTests(unittest.TestCase):
    def test_own_addresses_are_accepted(self):
        for header in ("127.0.0.1:8756", "localhost:8756", "LOCALHOST:8756"):
            self.assertTrue(host_allowed(header, "127.0.0.1", 8756), header)

    def test_a_rebinding_hostname_is_refused(self):
        self.assertFalse(host_allowed("evil.example:8756", "127.0.0.1", 8756))
        self.assertFalse(host_allowed("evil.example", "127.0.0.1", 8756))

    def test_another_port_is_refused(self):
        # The launcher's fallback port changes PORT; the old one is not ours.
        self.assertFalse(host_allowed("127.0.0.1:8757", "127.0.0.1", 8756))

    def test_a_missing_host_is_refused(self):
        self.assertFalse(host_allowed(None, "127.0.0.1", 8756))
        self.assertFalse(host_allowed("", "127.0.0.1", 8756))

    def test_ipv6_loopback_is_matched_in_brackets(self):
        self.assertTrue(host_allowed("[::1]:8756", "::1", 8756))
        self.assertTrue(host_allowed("localhost:8756", "::1", 8756))
        self.assertFalse(host_allowed("::1:8756", "::1", 8756))

    def test_a_wildcard_bind_accepts_any_host(self):
        self.assertIsNone(allowed_hosts("0.0.0.0", 8756))
        self.assertTrue(host_allowed("192.168.1.20:8756", "0.0.0.0", 8756))


class HostMiddlewareTests(unittest.TestCase):
    """The check is wired into the local server, not only defined."""

    # Other test modules leave stubs for these in sys.modules; the server must
    # be imported against the real packages.
    _REAL = ("fastapi", "starlette", "pydantic", "captions", "localapp")

    @classmethod
    def setUpClass(cls):
        import importlib
        import tempfile
        from unittest import mock

        cls._work = tempfile.TemporaryDirectory()
        with mock.patch.dict(os.environ, {"BOLCAP_WORK_DIR": cls._work.name}), \
                mock.patch.dict(sys.modules):
            for name in [n for n in sys.modules if n.split(".")[0] in cls._REAL]:
                del sys.modules[name]
            cls.server = importlib.import_module("localapp.server")
            cls.Request = importlib.import_module("starlette.requests").Request

    @classmethod
    def tearDownClass(cls):
        cls._work.cleanup()

    def _dispatch(self, host):
        import asyncio

        headers = [] if host is None else [(b"host", host.encode())]
        request = self.Request({"type": "http", "method": "GET", "path": "/api/setup/status",
                           "headers": headers, "query_string": b""})

        async def call_next(_request):
            return "reached the endpoint"

        return asyncio.run(self.server.reject_foreign_host(request, call_next))

    def test_middleware_is_registered_on_the_app(self):
        dispatches = [m.kwargs.get("dispatch") for m in self.server.app.user_middleware]
        self.assertIn(self.server.reject_foreign_host, dispatches)

    def test_a_rebinding_host_never_reaches_the_endpoint(self):
        response = self._dispatch("evil.example:%d" % self.server.PORT)
        self.assertEqual(response.status_code, 403)

    def test_our_own_host_reaches_the_endpoint(self):
        response = self._dispatch("127.0.0.1:%d" % self.server.PORT)
        self.assertEqual(response, "reached the endpoint")


if __name__ == "__main__":
    unittest.main()
