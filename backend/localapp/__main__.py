import threading
import webbrowser

import uvicorn

from .server import app, HOST, PORT

if __name__ == "__main__":
    threading.Timer(1.0, lambda: webbrowser.open(f"http://{HOST}:{PORT}")).start()
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
