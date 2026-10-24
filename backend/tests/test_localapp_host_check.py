import asyncio
import unittest
from unittest import mock

from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from starlette.requests import Request
from localapp import server


class HostCheckTest(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(server.app,
                                 base_url=f"http://127.0.0.1:{server.PORT}")

    def test_foreign_host_is_rejected(self):
        res = self.client.get("/api/setup/status",
                              headers={"host": "rebind.example"})
        self.assertEqual(res.status_code, 403)
        self.assertEqual(res.json()["detail"], "Host header is not allowed")

    def test_loopback_hosts_are_allowed(self):
        for host in (f"127.0.0.1:{server.PORT}",
                     f"localhost:{server.PORT}"):
            with self.subTest(host=host):
                res = self.client.get("/api/setup/status",
                                      headers={"host": host})
                self.assertEqual(res.status_code, 200)

    def test_missing_host_is_rejected(self):
        scope = {
            "type": "http", "method": "GET", "path": "/api/setup/status",
            "headers": [], "query_string": b"", "server": ("127.0.0.1",
                                                          server.PORT),
            "client": ("127.0.0.1", 12345), "scheme": "http",
        }
        called = []

        async def call_next(request):
            called.append(True)
            return JSONResponse({"ok": True})

        res = asyncio.run(server.block_foreign_host(Request(scope), call_next))
        self.assertEqual(res.status_code, 403)
        self.assertEqual(called, [])

    def test_ipv6_configured_host_is_bracketed(self):
        with mock.patch.object(server, "HOST", "::1"), \
                mock.patch.object(server, "PORT", 9123):
            self.assertIn("[::1]:9123", server._allowed_hosts())
            self.assertIn("http://[::1]:9123", server._allowed_origins())


if __name__ == "__main__":
    unittest.main()
