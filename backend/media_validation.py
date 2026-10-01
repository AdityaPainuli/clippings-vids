"""Validate uploaded media before expensive processing begins."""

import json
from pathlib import Path
import subprocess
from typing import Iterable


class MediaValidationError(ValueError):
    """Raised when an upload is missing, empty, or not usable media."""


def validate_media_file(
    path: str,
    *,
    required_streams: Iterable[str] = ("video",),
    ffprobe_command: str = "ffprobe",
) -> dict:
    """Return FFprobe metadata for a usable media file.

    Validation is based on the decoded container and streams, never the file
    extension. The clip pipeline requires a video stream by default.
    """
    media_path = Path(path)
    if not media_path.is_file():
        raise MediaValidationError("uploaded media file does not exist")
    if media_path.stat().st_size == 0:
        raise MediaValidationError("uploaded media file is empty")

    command = [
        ffprobe_command,
        "-v",
        "error",
        "-show_entries",
        "format=format_name,format_long_name:stream=codec_type",
        "-of",
        "json",
        str(media_path),
    ]
    try:
        probe = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except FileNotFoundError as error:
        raise MediaValidationError("media validation is unavailable: ffprobe not found") from error
    except subprocess.CalledProcessError as error:
        detail = (error.stderr or "").strip()
        message = "uploaded file is unsupported or corrupted"
        if detail:
            message = f"{message}: {detail[-200:]}"
        raise MediaValidationError(message) from error
    except subprocess.TimeoutExpired as error:
        raise MediaValidationError("media validation timed out") from error

    try:
        metadata = json.loads(probe.stdout)
    except (TypeError, json.JSONDecodeError) as error:
        raise MediaValidationError("ffprobe returned invalid media metadata") from error

    if not isinstance(metadata, dict):
        raise MediaValidationError("ffprobe returned invalid media metadata")

    format_metadata = metadata.get("format", {})
    if not isinstance(format_metadata, dict):
        raise MediaValidationError("ffprobe returned invalid media metadata")
    format_name = format_metadata.get("format_name")
    streams = {
        stream.get("codec_type")
        for stream in metadata.get("streams", [])
        if isinstance(stream, dict)
    }
    if not format_name:
        raise MediaValidationError("uploaded file has no recognized media container")

    required = set(required_streams)
    if required and not required.intersection(streams):
        expected = " or ".join(sorted(required))
        raise MediaValidationError(f"uploaded media must contain a valid {expected} stream")
    return metadata
