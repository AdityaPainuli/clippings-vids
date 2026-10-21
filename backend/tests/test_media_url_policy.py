import asyncio
import importlib.util
import os
import socket
import sys
import types
import unittest
from unittest import mock

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

import url_policy  # noqa: E402
from url_policy import UnsafeURLError, check_media_url  # noqa: E402


def _addrinfo(*ips):
    """Fake getaddrinfo() result for the given addresses."""
    out = []
    for ip in ips:
        if ":" in ip:
            out.append((socket.AF_INET6, socket.SOCK_STREAM, 6, "", (ip, 443, 0, 0)))
        else:
            out.append((socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 443)))
    return out


def _resolves_to(*ips):
    return mock.patch.object(url_policy.socket, "getaddrinfo", return_value=_addrinfo(*ips))


def _load_fresh(name, filename, stubs):
    """
    Import a backend module from its file under a private name, with `stubs`
    in sys.modules only for the duration of the import. Other test modules
    leave their own stubs for `clipper`, `supabase_client` and `fastapi` in
    sys.modules, so a plain import here would get whichever ran first.
    """
    spec = importlib.util.spec_from_file_location(name, os.path.join(BACKEND_DIR, filename))
    module = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, stubs):
        spec.loader.exec_module(module)
    return module


class CheckMediaUrlTest(unittest.TestCase):
    def test_public_youtube_url_is_allowed(self):
        url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
        with _resolves_to("142.250.183.110", "2404:6800:4009:82a::200e"):
            self.assertEqual(check_media_url(url), url)

    def test_plain_http_is_allowed(self):
        with _resolves_to("93.184.216.34"):
            self.assertEqual(check_media_url("http://vimeo.com/76979871"), "http://vimeo.com/76979871")

    def test_rejects_non_http_schemes(self):
        for url in ("file:///etc/passwd", "ftp://example.com/a.mp4",
                    "gopher://example.com/", "javascript:alert(1)", "youtube.com/watch?v=x"):
            with self.subTest(url=url), _resolves_to("93.184.216.34"):
                with self.assertRaisesRegex(UnsafeURLError, "http and https"):
                    check_media_url(url)

    def test_rejects_empty_and_malformed(self):
        for url in ("", "   ", None, "http://", "http://[::1", "http://example.com:99999/"):
            with self.subTest(url=url), _resolves_to("93.184.216.34"):
                with self.assertRaises(UnsafeURLError):
                    check_media_url(url)

    def test_rejects_embedded_credentials(self):
        with _resolves_to("93.184.216.34"):
            with self.assertRaisesRegex(UnsafeURLError, "username or password"):
                check_media_url("https://user:pass@example.com/video")

    def test_rejects_non_public_literal_addresses(self):
        # Real getaddrinfo: IP literals resolve without touching the network.
        for host in (
            "127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1",
            "169.254.169.254",          # cloud metadata, link-local
            "100.64.0.1",               # carrier-grade NAT
            "0.0.0.0", "224.0.0.1", "240.0.0.1", "255.255.255.255",
            "2130706433",               # 127.0.0.1 in decimal
            "[::1]", "[::]", "[fe80::1]", "[fc00::1]", "[ff02::1]",
            "[::ffff:127.0.0.1]", "[::ffff:10.0.0.1]", "[::ffff:169.254.169.254]",
        ):
            with self.subTest(host=host):
                with self.assertRaisesRegex(UnsafeURLError, "private or reserved"):
                    check_media_url(f"http://{host}/video.mp4")

    def test_rejects_hostname_resolving_to_private_address(self):
        with _resolves_to("10.1.2.3"):
            with self.assertRaisesRegex(UnsafeURLError, "private or reserved"):
                check_media_url("https://internal.example.com/video")

    def test_rejects_ipv4_mapped_ipv6_from_dns(self):
        with _resolves_to("::ffff:127.0.0.1"):
            with self.assertRaisesRegex(UnsafeURLError, "private or reserved"):
                check_media_url("https://sneaky.example.com/video")

    def test_rejects_mixed_public_and_private_records(self):
        with _resolves_to("93.184.216.34", "192.168.0.10"):
            with self.assertRaisesRegex(UnsafeURLError, "private or reserved"):
                check_media_url("https://mixed.example.com/video")

    def test_dns_failure_fails_closed(self):
        with mock.patch.object(url_policy.socket, "getaddrinfo",
                               side_effect=socket.gaierror("nodename nor servname provided")):
            with self.assertRaisesRegex(UnsafeURLError, "Could not resolve"):
                check_media_url("https://does-not-exist.example/video")


class DownloadVideoGateTest(unittest.TestCase):
    """clipper.download_video is the shared choke point for every yt-dlp fetch."""

    @classmethod
    def setUpClass(cls):
        stubs = {mod: mock.MagicMock() for mod in (
            "yt_dlp", "google", "google.generativeai", "moviepy", "dotenv")}
        cls.clipper = _load_fresh("_clipper_under_test", "clipper.py", stubs)

    def test_rejected_url_never_reaches_yt_dlp(self):
        ytdl = mock.MagicMock()
        with mock.patch.object(self.clipper, "yt_dlp", ytdl):
            with self.assertRaises(UnsafeURLError):
                self.clipper.download_video("http://169.254.169.254/latest/meta-data/", "unused")
        ytdl.YoutubeDL.assert_not_called()

    def test_allowed_url_limits_extractors_to_platforms(self):
        ytdl = mock.MagicMock()
        ydl = ytdl.YoutubeDL.return_value.__enter__.return_value
        ydl.extract_info.return_value = {"id": "abc"}
        ydl.prepare_filename.return_value = "unused/abc.mp4"
        with mock.patch.object(self.clipper, "yt_dlp", ytdl), \
             mock.patch.object(self.clipper.os, "makedirs"), \
             _resolves_to("142.250.183.110"):
            self.clipper.download_video("https://youtu.be/dQw4w9WgXcQ", "unused")

        opts = ytdl.YoutubeDL.call_args.args[0]
        # Allowlist only: "default" would pull in any-host extractors like
        # Mediasite, which an attacker's server can satisfy.
        self.assertEqual(opts["allowed_extractors"], ["youtube.*", "twitch.*", "instagram.*"])
        ydl.extract_info.assert_called_once_with("https://youtu.be/dQw4w9WgXcQ", download=True)


class ProcessUrlRouteTest(unittest.TestCase):
    """/process-url answers 400 instead of queueing a job that fetches internally."""

    class _HTTPException(Exception):
        def __init__(self, status_code, detail):
            self.status_code = status_code
            self.detail = detail

    @classmethod
    def setUpClass(cls):
        class _App:
            def include_router(self, router): pass
            def add_middleware(self, *a, **k): pass
            def get(self, *a, **k): return lambda f: f
            def post(self, *a, **k): return lambda f: f
            def delete(self, *a, **k): return lambda f: f

        fastapi = types.ModuleType("fastapi")
        fastapi.FastAPI = _App
        fastapi.HTTPException = cls._HTTPException
        for name in ("UploadFile", "BackgroundTasks", "Request"):
            setattr(fastapi, name, object)
        for name in ("File", "Form", "Depends"):
            setattr(fastapi, name, lambda *a, **k: None)
        security = types.ModuleType("fastapi.security")
        security.HTTPBearer = lambda *a, **k: object()
        security.HTTPAuthorizationCredentials = object

        clipper = types.ModuleType("clipper")
        clipper.CLIP_STYLES = {"auto": "Auto"}
        clipper.CAPTION_PRESETS = {"default": {}}

        captions = types.ModuleType("captions")
        captions.__path__ = []
        captions_api = types.ModuleType("captions.api")
        captions_api.router = object()

        stubs = {
            "fastapi": fastapi,
            "fastapi.middleware": types.ModuleType("fastapi.middleware"),
            "fastapi.middleware.cors": mock.MagicMock(),
            "fastapi.security": security,
            "fastapi.responses": mock.MagicMock(),
            "clipper": clipper,
            "supabase_client": mock.MagicMock(),
            "captions": captions,
            "captions.api": captions_api,
        }
        cls.main = _load_fresh("_main_under_test", "main.py", stubs)

    def _post(self, url):
        tasks = mock.MagicMock()
        coro = self.main.process_url(
            background_tasks=tasks, url=url, instructions=None, captions=True,
            clip_style="auto", caption_style="default", clip_count=None,
            min_clip_length=None, max_clip_length=None, user={"user_id": "user-1"},
        )
        return tasks, coro

    def test_internal_url_gets_400_and_no_job(self):
        jobs_before = dict(self.main.jobs)
        tasks, coro = self._post("http://127.0.0.1:8000/admin")
        with self.assertRaises(self._HTTPException) as ctx:
            asyncio.run(coro)
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("private or reserved", ctx.exception.detail)
        tasks.add_task.assert_not_called()
        self.assertEqual(self.main.jobs, jobs_before)

    def test_non_http_scheme_gets_400(self):
        tasks, coro = self._post("file:///etc/passwd")
        with self.assertRaises(self._HTTPException) as ctx:
            asyncio.run(coro)
        self.assertEqual(ctx.exception.status_code, 400)
        tasks.add_task.assert_not_called()

    def test_public_url_is_queued(self):
        tasks, coro = self._post("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
        with _resolves_to("142.250.183.110"), \
             mock.patch.object(self.main, "_maybe_cleanup", mock.AsyncMock()):
            result = asyncio.run(coro)
        self.assertEqual(result["status"], "queued")
        tasks.add_task.assert_called_once()


if __name__ == "__main__":
    unittest.main()
