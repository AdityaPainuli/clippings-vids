"""
Input validation for uploaded media files.

Uses FFprobe to verify that a file is a supported media container with at
least one audio or video stream before handing it to the processing pipeline.
"""

import json
import os
import subprocess

SUPPORTED_CODECS_VIDEO = {"h264", "hevc", "vp8", "vp9", "av1", "mpeg4", "theora"}
SUPPORTED_CODECS_AUDIO = {
    "aac", "mp3", "opus", "vorbis", "flac", "pcm_s16le", "pcm_s24le",
    "pcm_f32le", "pcm_s16be", "alac", "wmav2",
}


class MediaValidationError(Exception):
    pass


def validate_media_file(file_path: str, require_audio: bool = False) -> dict:
    """Validate a media file using FFprobe.

    Returns a dict with keys: duration, has_video, has_audio, video_codec,
    audio_codec, format_name.

    Raises MediaValidationError with a user-facing message on failure.
    """
    if not file_path or not os.path.isfile(file_path):
        raise MediaValidationError("File not found.")

    if os.path.getsize(file_path) == 0:
        raise MediaValidationError("File is empty.")

    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_format", "-show_streams",
                file_path,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except FileNotFoundError:
        raise MediaValidationError("FFprobe is not installed or not on PATH.")
    except subprocess.TimeoutExpired:
        raise MediaValidationError("Media probe timed out — file may be corrupt.")

    if result.returncode != 0:
        raise MediaValidationError("File is not a recognised media container.")

    try:
        probe = json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError):
        raise MediaValidationError("FFprobe returned unparseable output.")

    streams = probe.get("streams", [])
    if not streams:
        raise MediaValidationError("File contains no media streams.")

    video_streams = [s for s in streams if s.get("codec_type") == "video"]
    audio_streams = [s for s in streams if s.get("codec_type") == "audio"]

    has_video = len(video_streams) > 0
    has_audio = len(audio_streams) > 0

    if not has_video and not has_audio:
        raise MediaValidationError("File contains no audio or video streams.")

    if require_audio and not has_audio:
        raise MediaValidationError("File contains no audio stream.")

    video_codec = video_streams[0].get("codec_name") if has_video else None
    audio_codec = audio_streams[0].get("codec_name") if has_audio else None

    if has_video and video_codec and video_codec not in SUPPORTED_CODECS_VIDEO:
        raise MediaValidationError(
            f"Unsupported video codec: {video_codec}."
        )
    if has_audio and audio_codec and audio_codec not in SUPPORTED_CODECS_AUDIO:
        raise MediaValidationError(
            f"Unsupported audio codec: {audio_codec}."
        )

    fmt = probe.get("format", {})
    duration = float(fmt.get("duration", 0))

    return {
        "duration": duration,
        "has_video": has_video,
        "has_audio": has_audio,
        "video_codec": video_codec,
        "audio_codec": audio_codec,
        "format_name": fmt.get("format_name", ""),
    }
