"""Outbound URL policy for server-side media downloads."""

import ipaddress
import socket
from urllib.parse import urlsplit


_ALLOWED_SCHEMES = {"http", "https"}
_MAX_URL_LENGTH = 2048


def _is_public_address(address: str) -> bool:
    """Return True only for globally routable IP addresses."""
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return False

    return ip.is_global and not (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_unspecified
        or ip.is_reserved
    )


def _resolve_public_addresses(hostname: str) -> set[str]:
    """Resolve a hostname and return its publicly routable addresses."""
    try:
        results = socket.getaddrinfo(
            hostname,
            None,
            type=socket.SOCK_STREAM,
            proto=socket.IPPROTO_TCP,
        )
    except (OSError, ValueError) as exc:
        raise ValueError(f"Could not resolve download host '{hostname}'") from exc

    addresses = {result[4][0] for result in results if result[4]}
    if not addresses:
        raise ValueError(f"Could not resolve download host '{hostname}'")

    blocked = sorted(address for address in addresses if not _is_public_address(address))
    if blocked:
        raise ValueError(
            f"Download host '{hostname}' resolves to a non-public address"
        )

    return addresses


def validate_egress_url(url: str) -> str:
    """Validate a user-supplied download URL before server-side network access."""
    if not isinstance(url, str) or not url.strip():
        raise ValueError("Download URL is required")
    if len(url) > _MAX_URL_LENGTH:
        raise ValueError("Download URL is too long")

    parsed = urlsplit(url)
    scheme = parsed.scheme.lower()
    if scheme not in _ALLOWED_SCHEMES:
        raise ValueError("Download URL must use http or https")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Download URL must not contain embedded credentials")

    try:
        hostname = parsed.hostname
    except ValueError as exc:
        raise ValueError("Download URL contains an invalid hostname") from exc

    if not hostname:
        raise ValueError("Download URL must include a hostname")

    try:
        _resolve_public_addresses(hostname)
    except ValueError:
        raise

    return url
