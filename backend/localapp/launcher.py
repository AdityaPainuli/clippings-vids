"""Shared desktop launcher: choose a port, then open the browser when ready."""

import os
import socket
import sys
import threading
import time
import webbrowser

import uvicorn

PORT_ATTEMPTS = 20
STARTUP_TIMEOUT = 30.0


def _port_available(host: str, port: int) -> bool:
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    try:
        with socket.socket(family, socket.SOCK_STREAM) as probe:
            probe.bind((host, port))
        return True
    except OSError:
        return False


def pick_port(host: str, preferred: int, attempts: int = PORT_ATTEMPTS):
    """The first free port at or after `preferred`, or None."""
    last = min(65535, preferred + attempts - 1)
    for port in range(preferred, last + 1):
        if _port_available(host, port):
            return port
    return None


def browser_url(host: str, port: int) -> str:
    return f"http://[{host}]:{port}" if ":" in host else f"http://{host}:{port}"


def _open_when_started(server, url: str, open_browser, timeout: float):
    deadline = time.monotonic() + timeout
    while (not server.started and not server.should_exit
           and time.monotonic() < deadline):
        time.sleep(0.05)
    if server.started and not server.should_exit:
        open_browser(url)


def _report_no_port(host: str, preferred: int, attempts: int):
    last = min(65535, preferred + attempts - 1)
    print(f"[bolcap] no free port on {host} in {preferred}-{last}; "
          "set BOLCAP_PORT to a free port and try again", file=sys.stderr)
    if os.name == "nt":
        try:
            input("Press Enter to close...")
        except EOFError:
            pass


def serve(server_module, open_browser=webbrowser.open,
          attempts: int = PORT_ATTEMPTS,
          startup_timeout: float = STARTUP_TIMEOUT):
    """
    Bind a free port, then serve and open the UI once uvicorn is listening.

    `server_module` is localapp.server rather than just the app so the chosen
    fallback port can update its origin allowlist, which is read from the
    module global when a write arrives.
    """
    host, preferred = server_module.HOST, server_module.PORT
    port = pick_port(host, preferred, attempts)
    if port is None:
        _report_no_port(host, preferred, attempts)
        raise SystemExit(1)

    server_module.PORT = port
    if port != preferred:
        print(f"[bolcap] port {preferred} is in use; using {port}")

    config = uvicorn.Config(server_module.app, host=host, port=port,
                            log_level="warning")
    server = uvicorn.Server(config)
    url = browser_url(host, port)
    threading.Thread(target=_open_when_started,
                     args=(server, url, open_browser, startup_timeout),
                     daemon=True).start()
    server.run()
