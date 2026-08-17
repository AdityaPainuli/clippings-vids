"""
Export formats for finished captions.

Editors get to choose how captions leave the system:
  - .ass / .srt        — subtitle files for any player or NLE
  - burned MP4         — captions rendered into the video
  - alpha overlay .mov — transparent caption-only video (QuickTime RLE),
                         drops onto a Premiere/Final Cut/Resolve timeline
                         above the original footage, fully non-destructive
"""

import json
import os
import subprocess


def probe_video(video_path: str) -> dict:
    """Return {"width", "height", "duration", "fps"} via ffprobe."""
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=width,height,r_frame_rate",
         "-show_entries", "format=duration", "-of", "json", video_path],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {r.stderr[-300:]}")
    data = json.loads(r.stdout)
    if not data.get("streams"):
        raise RuntimeError("No video stream found — unsupported or corrupt file")
    stream = data["streams"][0]
    num, den = stream["r_frame_rate"].split("/")
    return {
        "width": int(stream["width"]),
        "height": int(stream["height"]),
        "duration": float(data["format"]["duration"]),
        "fps": round(int(num) / int(den), 3),
    }


def _ass_time_srt(s: float) -> str:
    h = int(s // 3600)
    m = int((s % 3600) // 60)
    sec = int(s % 60)
    ms = int((s % 1) * 1000)
    return f"{h:02d}:{m:02d}:{sec:02d},{ms:03d}"


def export_srt(words: list, out_path: str, words_per_line: int = 3,
               text_key: str = "text") -> str:
    """Plain SRT (no styling) — universal NLE/player import."""
    entries = []
    display = [w for w in words if (w.get(text_key) or w.get("text") or "").strip()]
    for i in range(0, len(display), words_per_line):
        chunk = display[i : i + words_per_line]
        text = " ".join((w.get(text_key) or w.get("text") or "") for w in chunk)
        entries.append(
            f"{len(entries) + 1}\n"
            f"{_ass_time_srt(chunk[0]['start'])} --> {_ass_time_srt(chunk[-1]['end'])}\n"
            f"{text}\n"
        )
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(entries))
    return out_path


def _safe_ass_path(ass_path: str) -> str:
    return ass_path.replace("\\", "/").replace(":", "\\:")


def burn_video(video_path: str, ass_path: str, out_path: str, crf: int = 20) -> str:
    """Render captions into the video (single re-encode, audio copied)."""
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vf", f"ass={_safe_ass_path(ass_path)}",
        "-c:v", "libx264", "-preset", "fast", "-crf", str(crf),
        "-c:a", "copy", out_path,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"Burn failed: {r.stderr[-400:]}")
    return out_path


def render_overlay(ass_path: str, out_path: str, width: int, height: int,
                   duration: float, fps: float = 30.0) -> str:
    """
    Captions on a transparent canvas → QuickTime RLE .mov with alpha.
    Editors layer this above their footage in any NLE; original video
    never gets re-encoded.
    """
    # alpha=1 makes libass write the text into the alpha channel too;
    # without it the canvas stays fully transparent.
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi",
        "-i", f"color=c=black@0.0:s={width}x{height}:r={fps}:d={duration},format=yuva420p",
        "-vf", f"ass={_safe_ass_path(ass_path)}:alpha=1",
        "-c:v", "qtrle", out_path,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"Overlay render failed: {r.stderr[-400:]}")
    return out_path
