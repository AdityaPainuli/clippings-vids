import os
import yt_dlp
import google.generativeai as genai
from moviepy import VideoFileClip
import json
import uuid
import base64
import subprocess
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed

from dotenv import load_dotenv

load_dotenv()


GENAI_API_KEY = os.getenv("GOOGLE_API_KEY")
if GENAI_API_KEY:
    genai.configure(api_key=GENAI_API_KEY)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_video(url, output_path="downloads"):
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    ydl_opts = {
        'format': 'bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best',
        'outtmpl': os.path.join(output_path, '%(id)s.%(ext)s'),
        'writeautomaticsub': True,
        'subtitlesformat': 'vtt',
        'quiet': True,
        'no_warnings': True,
    }

    print(f"Downloading video from {url} (max 720p)...")
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        filename = ydl.prepare_filename(info)
        print(f"Download complete: {filename}")
        return filename, info


# ---------------------------------------------------------------------------
# Analysis helpers
# ---------------------------------------------------------------------------

def _extract_keyframes(video_path, n_frames=8):
    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", video_path],
        capture_output=True, text=True
    )
    try:
        duration = float(probe.stdout.strip())
    except ValueError:
        duration = 60.0

    frames_b64 = []
    with tempfile.TemporaryDirectory() as tmpdir:
        interval = duration / (n_frames + 1)
        for i in range(1, n_frames + 1):
            ts = interval * i
            out_path = os.path.join(tmpdir, f"frame_{i:03d}.jpg")
            subprocess.run(
                ["ffmpeg", "-ss", str(ts), "-i", video_path,
                 "-frames:v", "1", "-q:v", "5", out_path, "-y"],
                capture_output=True
            )
            if os.path.exists(out_path):
                with open(out_path, "rb") as f:
                    frames_b64.append({
                        "timestamp": round(ts, 1),
                        "data": base64.b64encode(f.read()).decode("utf-8"),
                    })
    return frames_b64, duration


def _parse_vtt_to_text(vtt_path):
    if not vtt_path or not os.path.exists(vtt_path):
        return None

    lines = []
    with open(vtt_path, encoding="utf-8") as f:
        raw = f.readlines()

    current_time = None
    for line in raw:
        line = line.strip()
        if "-->" in line:
            current_time = line.split("-->")[0].strip()[:8]
        elif line and current_time and not line.startswith("WEBVTT") and not line[0].isdigit():
            clean = ""
            skip = False
            for ch in line:
                if ch == "<":
                    skip = True
                elif ch == ">":
                    skip = False
                elif not skip:
                    clean += ch
            clean = clean.strip()
            if clean:
                lines.append(f"[{current_time}] {clean}")
                current_time = None

    return "\n".join(lines) if lines else None


def _find_vtt_file(video_path, info):
    base = os.path.splitext(video_path)[0]
    for lang in ["en", "en-US", "en-GB"]:
        candidate = f"{base}.{lang}.vtt"
        if os.path.exists(candidate):
            return candidate

    parent = os.path.dirname(video_path)
    video_stem = os.path.basename(base)
    for fname in os.listdir(parent):
        if fname.startswith(video_stem) and fname.endswith(".vtt"):
            return os.path.join(parent, fname)
    return None


def _parse_gemini_json(text):
    text = text.strip()
    if "```" in text:
        parts = text.split("```")
        text = parts[1]
        if text.lower().startswith("json"):
            text = text[4:]
    return json.loads(text.strip())


# ---------------------------------------------------------------------------
# Analysis — fast path (no video upload to Gemini)
# ---------------------------------------------------------------------------

def analyze_video(video_path, user_instructions=None, info=None):
    if not GENAI_API_KEY:
        return [{"start_time": 0, "end_time": 10, "description": "Short clip (API Key missing)"}]

    vtt_path = _find_vtt_file(video_path, info or {})
    transcript_text = _parse_vtt_to_text(vtt_path)
    if transcript_text:
        print(f"Found transcript: {len(transcript_text.splitlines())} cue lines")
    else:
        print("No transcript available — using keyframes only")

    print("Extracting keyframes...")
    frames, duration = _extract_keyframes(video_path, n_frames=8)
    print(f"Extracted {len(frames)} keyframes from {duration:.1f}s video")

    focus = user_instructions or "High energy moments, key points, or funny/surprising parts"
    transcript_section = (
        f"\n\nTRANSCRIPT (timestamped):\n{transcript_text}"
        if transcript_text
        else "\n\n(No transcript available — use keyframes only)"
    )

    prompt = f"""You are a social-media clip editor. The video is {duration:.1f} seconds long.

Below are {len(frames)} evenly-spaced keyframes and a timestamped transcript (if available).
Use BOTH to identify the best shareable moments.{transcript_section}

Return ONLY a valid JSON array (no markdown, no explanation).
Each element must have:
  "start_time"  – float, seconds from the start of the video
  "end_time"    – float, seconds from the start (clip length 15-60 s)
  "description" – one sentence describing why this moment is great

Focus: {focus}

Rules:
- Clips must not overlap.
- start_time and end_time must be within [0, {duration:.1f}].
- Return 3 to 5 clips.
"""

    content_parts = [prompt]
    for frame in frames:
        content_parts.append({
            "inline_data": {
                "mime_type": "image/jpeg",
                "data": frame["data"],
            }
        })
        content_parts.append(f"[Keyframe at {frame['timestamp']}s]")

    print("Calling Gemini for analysis...")
    model = genai.GenerativeModel(model_name="gemini-2.5-pro")
    response = model.generate_content(content_parts)

    try:
        clips = _parse_gemini_json(response.text)
        print(f"Gemini returned {len(clips)} clips")
        return clips
    except Exception as e:
        print(f"Error parsing Gemini response: {e}")
        print(f"Raw response: {response.text}")
        return []


# ---------------------------------------------------------------------------
# Clip rendering — parallel + fast ffmpeg settings
# ---------------------------------------------------------------------------

def _render_single_clip(args):
    """
    Worker — runs in its own process so all clips encode simultaneously.
    Uses ffmpeg directly for the vertical conversion: scales the 16:9 source
    to fill the 9:16 canvas width, then pads top/bottom with a blurred+darkened
    version of the same frame so nothing is cropped out.
    """
    video_path, start, end, output_path = args
    try:
        duration = end - start

        # Target canvas: 1080x1920 (9:16)
        # vf filter breakdown:
        #   [0:v] split into two streams
        #   stream 1 (bg): scale to 1080 wide, blur heavily, darken — fills canvas
        #   stream 2 (fg): scale to fit inside 1080x1920 keeping aspect ratio
        #   overlay fg centred on bg
        vf = (
            "[0:v]split=2[bg][fg];"
            "[bg]scale=1080:1920:force_original_aspect_ratio=increase,"
            "crop=1080:1920,"
            "gblur=sigma=40,"
            "eq=brightness=-0.3[bg_blurred];"
            "[fg]scale=1080:1920:force_original_aspect_ratio=decrease,"
            "pad=1080:1920:(ow-iw)/2:(oh-ih)/2:color=black@0[fg_scaled];"
            "[bg_blurred][fg_scaled]overlay=0:0"
        )

        cmd = [
            "ffmpeg", "-y",
            "-ss", str(start),
            "-i", video_path,
            "-t", str(duration),
            "-vf", vf,
            "-c:v", "libx264",
            "-preset", "ultrafast",
            "-crf", "28",
            "-c:a", "aac",
            "-threads", "2",
            output_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(result.stderr[-500:])
        return output_path, None
    except Exception as e:
        return output_path, str(e)


def create_clips(video_path, clips_metadata, output_dir="output"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if not clips_metadata:
        print("No clip metadata returned from analysis.")
        return []

    # Probe duration once with ffprobe — faster than opening MoviePy
    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", video_path],
        capture_output=True, text=True
    )
    try:
        total_duration = float(probe.stdout.strip())
    except ValueError:
        total_duration = VideoFileClip(video_path).duration

    # Build task list with clamped timestamps
    tasks = []
    meta_map = {}
    for i, metadata in enumerate(clips_metadata):
        start = float(metadata.get("start_time", 0))
        end   = float(metadata.get("end_time", 10))
        start = max(0.0, min(start, total_duration))
        end   = max(start + 1.0, min(end, total_duration))

        output_filename = f"clip_{i}_{uuid.uuid4().hex[:8]}.mp4"
        output_path = os.path.join(output_dir, output_filename)
        tasks.append((video_path, start, end, output_path))
        meta_map[output_path] = {
            "filename": output_filename,
            "description": metadata.get("description", ""),
            "start_time": start,
            "end_time": end,
        }

    # Encode all clips in parallel — cap at 4 workers to avoid I/O saturation
    n_workers = min(len(tasks), 4)
    print(f"Rendering {len(tasks)} clips in parallel ({n_workers} workers)...")

    created_files = []
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(_render_single_clip, t): t for t in tasks}
        for future in as_completed(futures):
            output_path, err = future.result()
            if err:
                print(f"  x Failed {os.path.basename(output_path)}: {err}")
            else:
                print(f"  + Done:   {os.path.basename(output_path)}")
                created_files.append(meta_map[output_path])

    # Restore original order (as_completed returns in finish order)
    order = {t[3]: idx for idx, t in enumerate(tasks)}
    created_files.sort(key=lambda c: order.get(
        os.path.join(output_dir, c["filename"]), 999
    ))
    return created_files