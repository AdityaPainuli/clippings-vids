import threading
import webbrowser
import uvicorn
from .server import HOST, PORT, app
def open_browser() -> None:
    """Open the local application in the default web browser."""
    webbrowser.open(f"http://{HOST}:{PORT}")
def main() -> None:
    """Start the local FastAPI application."""
    threading.Timer(1.0, open_browser).start()
    uvicorn.run(
        app,
        host=HOST,
        port=PORT,
        log_level="warning",
    )
if __name__ == "__main__":
    main()
