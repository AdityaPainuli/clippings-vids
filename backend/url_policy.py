"""
Outbound policy for user-supplied media URLs.

Anything we hand to yt-dlp is fetched from the backend's own network, so a URL
pointing at localhost, the cloud metadata service or a private subnet would be
fetched on the caller's behalf. check_media_url() is the single gate: http(s)
only, no embedded credentials, and every address the host resolves to must be
public.

Known limit: the check resolves the host once and yt-dlp resolves it again when
it fetches, so a DNS record that flips in between (rebinding) or a redirect to
an internal host is not caught here. clipper.download_video also limits yt-dlp
to the YouTube, Twitch and Instagram extractors, so the hosts actually fetched
are theirs, not the attacker's.
"""

import ipaddress
import socket
from urllib.parse import urlsplit


ALLOWED_SCHEMES = {"http", "https"}


class UnsafeURLError(ValueError):
    """The URL is not one the backend is allowed to fetch."""


def _is_public(ip) -> bool:
    # ::ffff:127.0.0.1 is 127.0.0.1 on the wire; judge the IPv4 address.
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return ip.is_global and not (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast      # 224.0.0.0/4 counts as is_global
        or ip.is_unspecified
    )


def check_media_url(url: str) -> str:
    """Return the URL unchanged if the backend may fetch it, else raise UnsafeURLError."""
    if not isinstance(url, str) or not url.strip():
        raise UnsafeURLError("A video URL is required")
    url = url.strip()

    try:
        parts = urlsplit(url)
        host = parts.hostname
        port = parts.port or (443 if parts.scheme.lower() == "https" else 80)
    except ValueError:
        raise UnsafeURLError("The video URL is malformed")

    if parts.scheme.lower() not in ALLOWED_SCHEMES:
        raise UnsafeURLError("Only http and https video URLs are supported")
    if parts.username is not None or parts.password is not None:
        raise UnsafeURLError("Video URLs must not contain a username or password")
    if not host:
        raise UnsafeURLError("The video URL has no host")

    # getaddrinfo also normalises odd literals like 2130706433 or 0x7f.1, so
    # IP literals go through the same path as hostnames.
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except (OSError, UnicodeError, ValueError):
        raise UnsafeURLError(f"Could not resolve host '{host}'")
    if not infos:
        raise UnsafeURLError(f"Could not resolve host '{host}'")

    # Every record must be public: the fetcher may pick any of them.
    for info in infos:
        try:
            ip = ipaddress.ip_address(info[4][0])
        except ValueError:
            raise UnsafeURLError(f"Could not resolve host '{host}'")
        if not _is_public(ip):
            raise UnsafeURLError(
                f"Host '{host}' points to a private or reserved address and can't be fetched"
            )

    return url
