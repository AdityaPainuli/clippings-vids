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


def probe_audio(video_path: str) -> dict:
    """
    Return {"has_audio", "channels", "sample_rate"} for the first audio stream.

    Separate from probe_video because most of the pipeline does not care, and
    the one place that does — FCPXML, which declares channel counts the NLE
    trusts on relink — must not guess. A file with no audio stream reports
    has_audio False rather than a channel count of zero dressed up as mono.
    """
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "a:0",
         "-show_entries", "stream=channels,sample_rate", "-of", "json", video_path],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        return {"has_audio": False, "channels": None, "sample_rate": None}
    streams = json.loads(r.stdout or "{}").get("streams") or []
    if not streams:
        return {"has_audio": False, "channels": None, "sample_rate": None}
    s0 = streams[0]
    rate = s0.get("sample_rate")
    return {
        "has_audio": True,
        "channels": int(s0["channels"]) if s0.get("channels") else None,
        "sample_rate": int(rate) if rate else None,
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
    """
    Escape a path for use as an ffmpeg filter option value. Two parser
    levels apply (option value, then filtergraph): single-quote the value
    and escape colons once, per ffmpeg's filtergraph-escaping docs.
    Windows drive letters (C:/...) break without this.
    """
    p = ass_path.replace("\\", "/").replace("'", r"\'").replace(":", r"\:")
    return f"'{p}'"


def _ass_filter(ass_path: str, extra: str = "") -> str:
    """ass filter spec; CAPTIONS_FONTS_DIR adds bundled fonts for libass
    (fresh machines may lack Arial Black / Devanagari coverage)."""
    spec = f"ass=filename={_safe_ass_path(ass_path)}{extra}"
    fonts_dir = os.getenv("CAPTIONS_FONTS_DIR")
    if fonts_dir:
        spec += f":fontsdir={_safe_ass_path(fonts_dir)}"
    return spec


def burn_video(video_path: str, ass_path: str, out_path: str, crf: int = 20) -> str:
    """Render captions into the video (single re-encode, audio copied)."""
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vf", _ass_filter(ass_path),
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
        "-vf", _ass_filter(ass_path, ":alpha=1"),
        "-c:v", "qtrle", out_path,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"Overlay render failed: {r.stderr[-400:]}")
    return out_path


def render_cut(video_path: str, keep: list, out_path: str,
               crf: int = 20, fade: float = 0.015) -> str:
    """
    Render only the kept spans, concatenated, from `keep` = [(start, end), ...].

    Splicing audio at arbitrary points pops, so every segment gets a ~15ms
    fade at each edge — inaudible as a fade, and the difference between
    "sounds edited" and "sounds broken". Cuts are frame-accurate, which
    means a real re-encode; stream copy can only cut on keyframes.
    """
    if not keep:
        raise RuntimeError("Nothing left to render — every span was cut")

    parts, vlabels, alabels = [], [], []
    for i, (s, e) in enumerate(keep):
        dur = e - s
        f = min(fade, dur / 3) if dur > 0 else 0
        parts.append(f"[0:v]trim=start={s:.3f}:end={e:.3f},setpts=PTS-STARTPTS[v{i}]")
        afilters = [f"atrim=start={s:.3f}:end={e:.3f}", "asetpts=PTS-STARTPTS"]
        if f > 0:
            afilters.append(f"afade=t=in:st=0:d={f:.3f}")
            afilters.append(f"afade=t=out:st={max(0, dur - f):.3f}:d={f:.3f}")
        parts.append(f"[0:a]{','.join(afilters)}[a{i}]")
        vlabels.append(f"[v{i}]")
        alabels.append(f"[a{i}]")

    pairs = "".join(v + a for v, a in zip(vlabels, alabels))
    parts.append(f"{pairs}concat=n={len(keep)}:v=1:a=1[vout][aout]")
    filtergraph = ";".join(parts)

    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-filter_complex", filtergraph,
        "-map", "[vout]", "-map", "[aout]",
        "-c:v", "libx264", "-preset", "fast", "-crf", str(crf),
        "-c:a", "aac", out_path,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"Cut render failed: {r.stderr[-400:]}")
    return out_path
