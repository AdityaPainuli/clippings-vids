import socket
import unittest
from unittest.mock import patch

from egress import _is_public_address, _resolve_public_addresses, validate_egress_url


class EgressPolicyTests(unittest.TestCase):
    def test_allows_http_and_https_public_host(self):
        with patch("egress.socket.getaddrinfo", return_value=[
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("93.184.216.34", 0)),
        ]):
            self.assertEqual(
                validate_egress_url("https://example.com/video"),
                "https://example.com/video",
            )

    def test_rejects_non_http_schemes(self):
        for url in (
            "file:///etc/passwd",
            "ftp://example.com/video",
            "javascript:alert(1)",
        ):
            with self.subTest(url=url):
                with self.assertRaisesRegex(ValueError, "must use http or https"):
                    validate_egress_url(url)

    def test_rejects_embedded_credentials(self):
        with self.assertRaisesRegex(ValueError, "embedded credentials"):
            validate_egress_url("https://user:password@example.com/video")

    def test_rejects_literal_private_address(self):
        with self.assertRaisesRegex(ValueError, "non-public address"):
            validate_egress_url("http://127.0.0.1:8080/video")

    def test_rejects_literal_ipv6_loopback(self):
        with self.assertRaisesRegex(ValueError, "non-public address"):
            validate_egress_url("http://[::1]:8080/video")

    def test_rejects_hostname_resolving_to_private_address(self):
        with patch("egress.socket.getaddrinfo", return_value=[
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("10.0.0.8", 0)),
        ]):
            with self.assertRaisesRegex(ValueError, "non-public address"):
                validate_egress_url("https://internal.example/video")

    def test_rejects_hostname_with_mixed_public_and_private_addresses(self):
        with patch("egress.socket.getaddrinfo", return_value=[
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("93.184.216.34", 0)),
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("192.168.1.5", 0)),
        ]):
            with self.assertRaisesRegex(ValueError, "non-public address"):
                validate_egress_url("https://mixed.example/video")

    def test_rejects_dns_resolution_failure(self):
        with patch("egress.socket.getaddrinfo", side_effect=socket.gaierror("temporary failure")):
            with self.assertRaisesRegex(ValueError, "Could not resolve download host"):
                validate_egress_url("https://unknown.example/video")

    def test_public_address_classifier_rejects_reserved_ranges(self):
        for address in (
            "127.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.0.1",
            "169.254.1.1",
            "::1",
            "fc00::1",
            "fe80::1",
        ):
            with self.subTest(address=address):
                self.assertFalse(_is_public_address(address))

    def test_public_address_classifier_allows_public_ipv4(self):
        self.assertTrue(_is_public_address("93.184.216.34"))

    def test_resolver_rejects_any_non_public_result(self):
        with patch("egress.socket.getaddrinfo", return_value=[
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("93.184.216.34", 0)),
            (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("::1", 0, 0, 0)),
        ]):
            with self.assertRaisesRegex(ValueError, "non-public address"):
                _resolve_public_addresses("example.com")


if __name__ == "__main__":
    unittest.main()
