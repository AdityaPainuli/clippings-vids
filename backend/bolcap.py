"""Bolcap launcher — PyInstaller entry point (also works as `python bolcap.py`)."""

from localapp import server
from localapp.launcher import serve

if __name__ == "__main__":
    serve(server)
