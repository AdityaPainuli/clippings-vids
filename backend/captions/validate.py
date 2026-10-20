# backend/captions/validate.py

import os
import subprocess
import json
import logging

logger = logging.getLogger(__name__)

class InvalidMediaError(Exception):
    """Raised when an uploaded file is missing, empty, corrupted, or lacks required media streams."""
    pass

def validate_media_file(file_path: str) -> bool:
    """
    Validates an input media file before pipeline processing.
    Checks:
    1. File existence.
    2. Non-zero file size.
    3. Valid container/format structure via ffprobe.
    4. Presence of at least one valid audio or video stream.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Media file not found at path: {file_path}")
    
    if os.path.getsize(file_path) == 0:
        raise InvalidMediaError("Uploaded media file is empty (0 bytes).")

    # Run ffprobe to inspect streams and container format
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        "-show_format",
        file_path
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=10
        )
        probe_data = json.loads(result.stdout)
    except subprocess.TimeoutExpired:
        raise InvalidMediaError("Media probing timed out. The file may be corrupt or excessively large.")
    except subprocess.CalledProcessError as e:
        logger.error(f"ffprobe failed for {file_path}: {e.stderr}")
        raise InvalidMediaError(f"File is not a valid media container or is corrupted: {e.stderr.strip()}")
    except json.JSONDecodeError:
        raise InvalidMediaError("Failed to parse ffprobe output structure.")
    except Exception as err:
        raise InvalidMediaError(f"Unexpected error while validating media file: {str(err)}")

    streams = probe_data.get("streams", [])
    if not streams:
        raise InvalidMediaError("Media file contains no readable streams.")

    # Verify presence of audio or video stream
    has_audio = any(s.get("codec_type") == "audio" for s in streams)
    has_video = any(s.get("codec_type") == "video" for s in streams)

    if not (has_audio or has_video):
        raise InvalidMediaError("Media file must contain at least one valid audio or video stream.")

    return True
