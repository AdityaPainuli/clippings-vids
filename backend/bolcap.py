"""Bolcap launcher — PyInstaller entry point (also works as `python bolcap.py`)."""

import threading
import webbrowser

import uvicorn

from localapp.server import app, HOST, PORT

if __name__ == "__main__":
    threading.Timer(1.0, lambda: webbrowser.open(f"http://{HOST}:{PORT}")).start()
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
