import subprocess
import json

class InvalidMediaError(Exception):
    """Raised when uploaded file is not a valid, usable media file."""
    pass


def validate_media_file(file_path: str) -> dict:
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration:stream=codec_type",
                "-of", "json",
                file_path,
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except FileNotFoundError:
        raise InvalidMediaError("ffprobe not found on server")
    except subprocess.TimeoutExpired:
        raise InvalidMediaError("File took too long to inspect — likely corrupted")

    if result.returncode != 0:
        raise InvalidMediaError("File is not a readable media file")

    try:
        info = json.loads(result.stdout)
    except json.JSONDecodeError:
        raise InvalidMediaError("Could not read file metadata")

    streams = info.get("streams", [])
    has_video = any(s.get("codec_type") == "video" for s in streams)
    has_audio = any(s.get("codec_type") == "audio" for s in streams)

    if not has_video and not has_audio:
        raise InvalidMediaError("File has no audio or video stream")

    duration = float(info.get("format", {}).get("duration", 0))
    if duration <= 0:
        raise InvalidMediaError("File has zero or unknown duration — likely empty/corrupted")

    return {"duration": duration, "has_video": has_video, "has_audio": has_audio}