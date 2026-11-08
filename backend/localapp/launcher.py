import socket
import threading
import webbrowser

import uvicorn

from .server import app, HOST, PORT


def find_available_port(host, start_port):
    port = start_port

    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind((host, port))
                return port
            except OSError:
                port += 1


class BrowserServer(uvicorn.Server):
    async def startup(self, sockets=None):
        await super().startup(sockets=sockets)

        if self.started:
            threading.Timer(
                1.0,
                lambda: webbrowser.open(
                    f"http://{HOST}:{self.config.port}"
                ),
            ).start()


def run():
    port = find_available_port(HOST, PORT)

    config = uvicorn.Config(
        app,
        host=HOST,
        port=port,
        log_level="warning",
    )

    server = BrowserServer(config)
    server.run()


if __name__ == "__main__":
    run()
