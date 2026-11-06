"""
Which `Host` headers the local server answers.

Binding to loopback keeps other machines out, but not other web pages: a
DNS-rebinding page points its own hostname at 127.0.0.1, and the browser then
sends its reads to this server with that hostname in `Host`. The Origin check
on writes does not see those reads, and `/api/setup/status` hands back the home
path, the key provider and the key's last four characters. Refusing any `Host`
that is not one of our own addresses stops the rebinding page from reading or
writing.
"""

from typing import Optional

# Wildcard binds (LAN opt-in) are reached by whatever address the machine has,
# so there is no fixed host list to hold requests to.
_WILDCARD = ("0.0.0.0", "::")


def allowed_hosts(host: str, port: int) -> Optional[set]:
    """The `Host` values to accept, lower-cased, or None to accept any."""
    if host in _WILDCARD:
        return None
    names = {host, "127.0.0.1", "localhost"}
    if host == "::1":
        names.discard("::1")
        names.add("[::1]")
    return {f"{name}:{port}".lower() for name in names}


def host_allowed(header: Optional[str], host: str, port: int) -> bool:
    """Whether a request carrying `Host: header` is addressed to this server."""
    allowed = allowed_hosts(host, port)
    if allowed is None:
        return True
    if not header:
        return False
    return header.strip().lower() in allowed
