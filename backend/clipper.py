import os
import yt_dlp
import google.generativeai as genai
from moviepy import VideoFileClip
import json
import uuid
import base64
import subprocess
import tempfile
import re
from concurrent.futures import ProcessPoolExecutor, as_completed

from dotenv import load_dotenv

load_dotenv()


GENAI_API_KEY = os.getenv("GOOGLE_API_KEY")
if GENAI_API_KEY:
    genai.configure(api_key=GENAI_API_KEY)

# Caption style — override via env vars
# ASS colours are &HAABBGGRR  (alpha, blue, green, red)
CAPTION_FONT         = os.getenv("CAPTION_FONT",        "Arial Black")
CAPTION_FONTSIZE     = int(os.getenv("CAPTION_FONTSIZE",    "72"))
CAPTION_COLOR        = os.getenv("CAPTION_COLOR",       "&H00FFFFFF")   # default word: white
CAPTION_HIGHLIGHT    = os.getenv("CAPTION_HIGHLIGHT",   "&H0000FFFF")   # active word: yellow
CAPTION_OUTLINE_CLR  = os.getenv("CAPTION_OUTLINE_CLR", "&H00000000")   # outline: black
CAPTION_SHADOW_CLR   = os.getenv("CAPTION_SHADOW_CLR",  "&H66000000")   # soft drop-shadow
CAPTION_OUTLINE_W    = int(os.getenv("CAPTION_OUTLINE_W",  "4"))         # outline thickness px
CAPTION_SHADOW_W     = int(os.getenv("CAPTION_SHADOW_W",   "3"))         # shadow depth px
CAPTION_WORDS        = int(os.getenv("CAPTION_WORDS",      "3"))         # words per line
CAPTION_MARGIN_V     = int(os.getenv("CAPTION_MARGIN_V",   "320"))       # px from bottom


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_video(url, output_path="downloads"):
    os.makedirs(output_path, exist_ok=True)
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
            clean = re.sub(r"<[^>]+>", "", line).strip()
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
    for fname in os.listdir(parent):
        if fname.startswith(os.path.basename(base)) and fname.endswith(".vtt"):
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
# Analysis
# ---------------------------------------------------------------------------

def _detect_content_type(transcript_text: str, info: dict) -> str:
    """
    Infer content type from transcript language patterns and yt-dlp metadata.
    Used to tailor the virality prompt to the specific genre.
    """
    title       = (info.get("title") or "").lower()
    description = (info.get("description") or "").lower()
    tags        = " ".join(info.get("tags") or []).lower()
    combined    = f"{title} {description} {tags} {(transcript_text or '')[:500]}".lower()

    if any(w in combined for w in ["podcast", "interview", "episode", "guest", "host"]):
        return "podcast/interview"
    if any(w in combined for w in ["tutorial", "how to", "step by step", "learn", "guide", "course"]):
        return "tutorial/educational"
    if any(w in combined for w in ["react", "reacts", "reaction", "watching"]):
        return "reaction"
    if any(w in combined for w in ["vlog", "day in", "daily", "routine"]):
        return "vlog"
    if any(w in combined for w in ["comedy", "funny", "prank", "roast", "sketch"]):
        return "comedy"
    if any(w in combined for w in ["motivation", "mindset", "success", "hustle", "advice"]):
        return "motivational/advice"
    if any(w in combined for w in ["news", "breaking", "report", "politics", "election"]):
        return "news/commentary"
    return "general"


def _virality_criteria_for_type(content_type: str) -> str:
    """
    Return genre-specific virality signals so Gemini knows exactly
    what to look for rather than guessing.
    """
    criteria = {
        "podcast/interview": """
- Moments where the guest says something surprising, controversial, or highly quotable
- Strong opinion stated with conviction ("The truth is...", "Nobody talks about this...")
- Personal story with emotional weight or unexpected reveal
- A back-and-forth exchange that peaks with a punchline or insight
- Moments where the host reacts visibly (laughs hard, leans in, says "wait what")
- Avoid: intros, small talk, topic transitions, sponsor reads""",

        "tutorial/educational": """
- The single most surprising or counterintuitive insight in the video
- The "aha moment" — when the concept clicks with a concrete example
- A demonstration where the result is visually striking or impressive
- A common mistake being called out ("Everyone does this wrong...")
- A before/after transformation moment
- Avoid: setup, prerequisites, "today we're going to learn", conclusions""",

        "reaction": """
- The peak reaction moment — biggest laugh, shock, or disbelief
- When the reactor says something that adds genuine insight to what they're watching
- A prediction that turns out to be wrong (funny subversion)
- Avoid: long silent watching segments, low-energy commentary""",

        "comedy": """
- The punchline and the 5-10 seconds of setup immediately before it
- A callback to an earlier joke that lands harder
- Moments of physical comedy or unexpected visual gag
- The most quotable one-liner in the video
- Avoid: slow builds without payoff, filler between bits""",

        "motivational/advice": """
- A single powerful piece of advice stated clearly and concisely
- A personal story with a strong lesson at the end
- A statement that challenges conventional wisdom directly
- A moment that would make someone stop scrolling and share
- Avoid: generic platitudes, rambling buildup, repetitive points""",

        "vlog": """
- An unexpected thing that happened — something that went wrong or surprisingly right
- A genuine emotional moment (real reaction, not performed)
- The most visually interesting or location-specific scene
- A funny or relatable observation about daily life
- Avoid: driving/commuting footage, meal prep without narration, filler transitions""",

        "news/commentary": """
- The sharpest, most pointed take or argument in the video
- A fact or statistic that would genuinely surprise most viewers
- A moment where the commentator's emotion is authentic and strong
- Avoid: context-setting intros, recapping known facts""",

        "general": """
- A moment that could stand completely alone without needing the rest of the video
- Something that would make a viewer tag a friend
- A strong opening hook — a question, bold statement, or surprising visual
- An emotional peak — biggest laugh, shock, insight, or tension
- Avoid: slow intros, recaps, outros, anything that requires prior context""",
    }
    return criteria.get(content_type, criteria["general"])


def analyze_video(video_path, user_instructions=None, info=None):
    if not GENAI_API_KEY:
        return [{"start_time": 0, "end_time": 10, "description": "Short clip (API Key missing)"}]

    # ── 1. Transcript — primary signal ───────────────────────────────────────
    vtt_path        = _find_vtt_file(video_path, info or {})
    transcript_text = _parse_vtt_to_text(vtt_path)
    has_transcript  = bool(transcript_text)
    if has_transcript:
        print(f"Transcript: {len(transcript_text.splitlines())} lines")
    else:
        print("No transcript — keyframes only")

    # ── 2. Keyframes — denser sampling when no transcript ────────────────────
    # With a transcript Gemini has exact timestamps so 12 frames is enough context.
    # Without a transcript we need more frames to locate moments accurately.
    n_frames = 12 if has_transcript else 20
    print(f"Extracting {n_frames} keyframes...")
    frames, duration = _extract_keyframes(video_path, n_frames=n_frames)
    print(f"Extracted {len(frames)} keyframes from {duration:.1f}s video")

    # ── 3. Content type detection ─────────────────────────────────────────────
    content_type   = _detect_content_type(transcript_text or "", info or {})
    virality_guide = _virality_criteria_for_type(content_type)
    print(f"Content type detected: {content_type}")

    # ── 4. Build prompt ───────────────────────────────────────────────────────
    video_title = (info or {}).get("title", "")
    title_line  = f'Video title: "{video_title}"\n' if video_title else ""

    transcript_section = (
        f"\n\nFULL TRANSCRIPT (timestamped — use these timestamps directly):\n{transcript_text}"
        if has_transcript
        else "\n\n(No transcript available — infer timing from keyframes)"
    )

    custom_focus = (
        f"\n\nCREATOR INSTRUCTIONS (prioritise these above all else):\n{user_instructions}"
        if user_instructions else ""
    )

    prompt = f"""You are an expert viral social-media clip editor with deep knowledge of what makes content perform on TikTok, Instagram Reels, and YouTube Shorts.

{title_line}Video duration: {duration:.1f} seconds
Content type: {content_type}

YOUR TASK:
Identify the 3-5 moments in this video that would perform best as standalone short-form clips. Each clip must be able to stand completely alone — a viewer who has never seen this video should immediately understand it and feel compelled to watch to the end.

VIRALITY SIGNALS TO LOOK FOR (specific to {content_type}):
{virality_guide}

UNIVERSAL RULES FOR CLIP SELECTION:
1. HOOK FIRST — the clip must start at or just before something attention-grabbing.    Never start mid-sentence or mid-thought.
2. COMPLETE THOUGHT — the clip must end at a natural conclusion (punchline, insight delivered,    story resolved). Never cut off mid-sentence.
3. NO CONTEXT REQUIRED — avoid anything that references "earlier" or "as I mentioned" —    the clip must be self-contained.
4. EMOTIONAL ARC — the best clips have a mini arc: setup → tension/curiosity → payoff.
5. QUOTABILITY — prioritise moments with a line someone would screenshot or repeat.{transcript_section}{custom_focus}

KEYFRAMES are provided below for visual context — use them to verify the scene but {"rely on transcript timestamps for precise clip boundaries." if has_transcript else "use them as your primary timing signal."}

Return ONLY a valid JSON array. No markdown, no explanation, no preamble.
Each object must have exactly these fields:
  "start_time"    – float, seconds (start just BEFORE the hook, not at it)
  "end_time"      – float, seconds (end AFTER the payoff, not before it)
  "description"   – one punchy sentence saying why this specific moment is viral
  "hook"          – the first words/action of the clip that will stop a scroller
  "virality_score"– integer 1-10 (10 = would go viral, 1 = boring)
  "clip_type"     – one of: insight|story|funny|reaction|tutorial|controversial|emotional

Constraints:
- Clips must not overlap
- start_time and end_time within [0, {duration:.1f}]
- Each clip between 20 and 60 seconds (shorter = better if the moment is complete)
- Return highest virality_score clips first
"""

    content_parts = [prompt]
    for frame in frames:
        content_parts.append({
            "inline_data": {"mime_type": "image/jpeg", "data": frame["data"]}
        })
        content_parts.append(f"[Keyframe at {frame['timestamp']}s]")

    print("Calling Gemini for viral moment analysis...")
    model    = genai.GenerativeModel(model_name="gemini-2.5-pro")
    response = model.generate_content(content_parts)

    try:
        clips = _parse_gemini_json(response.text)
        # Sort by virality score descending so best clip is always first
        clips.sort(key=lambda c: c.get("virality_score", 0), reverse=True)
        print(f"Gemini returned {len(clips)} clips:")
        for c in clips:
            print(f"  [{c.get('virality_score', '?')}/10] {c.get('clip_type','?')} "
                  f"{c.get('start_time')}s-{c.get('end_time')}s — {c.get('description','')[:60]}")
        return clips
    except Exception as e:
        print(f"Error parsing Gemini response: {e}\nRaw: {response.text}")
        return []


# ---------------------------------------------------------------------------
# Caption generation  (Whisper → ASS subtitle file)
# ---------------------------------------------------------------------------

def _seconds_to_ass_time(s: float) -> str:
    """Convert float seconds to ASS timestamp H:MM:SS.cc"""
    h  = int(s // 3600)
    m  = int((s % 3600) // 60)
    sc = s % 60
    return f"{h}:{m:02d}:{sc:05.2f}"


def _build_ass_header() -> str:
    """
    Two styles:
      Caption   – static words, white, thick black outline + drop shadow
      Highlight – active word, yellow, slightly larger
    BorderStyle 1 = outline+shadow (cleaner than opaque box).
    Alignment 2 = bottom-center.
    """
    # Build with plain concatenation — no f-string so backslash ASS tags are safe
    outline_w = str(CAPTION_OUTLINE_W)
    shadow_w  = str(CAPTION_SHADOW_W)
    margin_v  = str(CAPTION_MARGIN_V)
    size      = str(CAPTION_FONTSIZE)
    big_size  = str(int(CAPTION_FONTSIZE * 1.12))

    fmt = ("Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
           "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
           "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
           "Alignment, MarginL, MarginR, MarginV, Encoding\n")

    # common tail: Bold=-1, rest 0, ScaleX/Y=100, Spacing=2, Angle=0, BorderStyle=1
    tail = "-1,0,0,0,100,100,2,0,1," + outline_w + "," + shadow_w + ",2,40,40," + margin_v + ",1\n"

    caption_style   = ("Style: Caption,"   + CAPTION_FONT + "," + size     + ","
                       + CAPTION_COLOR    + ",&H000000FF,"
                       + CAPTION_OUTLINE_CLR + "," + CAPTION_SHADOW_CLR + "," + tail)
    highlight_style = ("Style: Highlight," + CAPTION_FONT + "," + big_size + ","
                       + CAPTION_HIGHLIGHT + ",&H000000FF,"
                       + CAPTION_OUTLINE_CLR + "," + CAPTION_SHADOW_CLR + "," + tail)

    return ("[Script Info]\n"
            "ScriptType: v4.00+\n"
            "PlayResX: 1080\n"
            "PlayResY: 1920\n"
            "WrapStyle: 1\n\n"
            "[V4+ Styles]\n"
            + fmt
            + caption_style
            + highlight_style
            + "\n[Events]\n"
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n")


def _tag(colour: str, alpha: str, scx: str = "100", scy: str = "100") -> str:
    """Build an ASS inline override tag string safely (no f-string backslash issues)."""
    return "{" + "\\c" + colour + "\\alpha" + alpha + "\\fscx" + scx + "\\fscy" + scy + "}"


def _whisper_transcribe(audio_path: str, clip_duration: float) -> list:
    """
    Run openai-whisper and return word-level segments.
    Returns: [{"start": float, "end": float, "text": str}, ...]
    """
    try:
        import whisper
    except ImportError:
        print("  [captions] whisper not installed — pip install openai-whisper")
        return []

    print("  [captions] Transcribing with Whisper...")
    model  = whisper.load_model("base")
    result = model.transcribe(audio_path, word_timestamps=True, fp16=False, verbose=False)

    words = []
    for seg in result.get("segments", []):
        for w in seg.get("words", []):
            words.append({
                "start": float(w["start"]),
                "end":   min(float(w["end"]), clip_duration),
                "text":  w["word"].strip(),
            })
    # Fallback to sentence segments
    if not words:
        for seg in result.get("segments", []):
            words.append({
                "start": float(seg["start"]),
                "end":   min(float(seg["end"]), clip_duration),
                "text":  seg["text"].strip(),
            })
    return words


def _words_to_ass_events(words: list) -> str:
    """
    One Dialogue line per word in each N-word chunk.
    Past words = white, current word = yellow + bigger, future words = dimmed white.
    Uses string concatenation for ASS override tags to avoid Python escape issues.
    """
    if not words:
        return ""

    WHITE      = "&H00FFFFFF"
    YELLOW     = "&H0000FFFF"
    FULL_ALPHA = "&H00"
    DIM_ALPHA  = "&HAA"        # ~33% opacity for upcoming words

    event_lines = []

    for i in range(0, len(words), CAPTION_WORDS):
        chunk       = words[i : i + CAPTION_WORDS]
        chunk_end   = chunk[-1]["end"]
        word_texts  = [w["text"].upper().replace("{", "").replace("}", "") for w in chunk]

        for active_idx, active_word in enumerate(chunk):
            seg_start = active_word["start"]
            seg_end   = (chunk[active_idx + 1]["start"]
                         if active_idx + 1 < len(chunk) else chunk_end)
            if seg_end <= seg_start:
                seg_end = seg_start + 0.1

            parts = []
            for j, wt in enumerate(word_texts):
                if j < active_idx:
                    parts.append(_tag(WHITE, FULL_ALPHA) + wt)
                elif j == active_idx:
                    parts.append(_tag(YELLOW, FULL_ALPHA, "115", "115") + wt
                                 + _tag(WHITE, FULL_ALPHA, "100", "100"))
                else:
                    parts.append(_tag(WHITE, DIM_ALPHA) + wt)

            line_text = (" ".join(parts)
                         + _tag(WHITE, FULL_ALPHA))  # reset for next chunk

            event_lines.append(
                "Dialogue: 0,"
                + _seconds_to_ass_time(seg_start) + ","
                + _seconds_to_ass_time(seg_end)
                + ",Caption,,0,0,0,," + line_text
            )

    return "\n".join(event_lines)


def generate_captions_ass(video_path: str, clip_duration: float, output_ass: str) -> bool:
    """Extract audio, transcribe with Whisper, write .ass file. Returns True on success."""
    with tempfile.TemporaryDirectory() as tmpdir:
        audio_path = os.path.join(tmpdir, "audio.wav")
        r = subprocess.run(
            ["ffmpeg", "-y", "-i", video_path, "-ac", "1", "-ar", "16000", "-vn", audio_path],
            capture_output=True
        )
        if r.returncode != 0 or not os.path.exists(audio_path):
            print("  [captions] Audio extraction failed")
            return False
        words = _whisper_transcribe(audio_path, clip_duration)

    if not words:
        print("  [captions] No words transcribed")
        return False

    ass_content = _build_ass_header() + _words_to_ass_events(words)
    with open(output_ass, "w", encoding="utf-8") as f:
        f.write(ass_content)

    print(f"  [captions] {len(words)} word(s) written to {os.path.basename(output_ass)}")
    return True

# ---------------------------------------------------------------------------
# Clip rendering — parallel, blur-pad background, burned captions
# ---------------------------------------------------------------------------

def _render_single_clip(args):
    """
    Worker process:
      1. Render vertical 9:16 clip with blurred background (ffmpeg)
      2. Transcribe audio with Whisper → write .ass subtitle file
      3. Burn captions into the clip (second ffmpeg pass)
    """
    video_path, start, end, output_path, captions_enabled = args
    try:
        duration = end - start

        # ── Pass 1: vertical conversion with blur-pad background ──────────────
        raw_path = output_path.replace(".mp4", "_raw.mp4")

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

        cmd1 = [
            "ffmpeg", "-y",
            "-ss", str(start), "-i", video_path,
            "-t", str(duration),
            "-vf", vf,
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
            "-c:a", "aac", "-threads", "2",
            raw_path,
        ]
        r1 = subprocess.run(cmd1, capture_output=True, text=True)
        if r1.returncode != 0:
            raise RuntimeError(f"Pass 1 failed: {r1.stderr[-400:]}")

        if not captions_enabled:
            os.rename(raw_path, output_path)
            return output_path, None

        # ── Pass 2: burn captions ─────────────────────────────────────────────
        ass_path = output_path.replace(".mp4", ".ass")
        has_captions = generate_captions_ass(raw_path, duration, ass_path)

        if not has_captions:
            # Whisper unavailable or no speech — just use the raw clip
            os.rename(raw_path, output_path)
            return output_path, None

        # Escape ass_path for ffmpeg filter (Windows backslash + colon issues)
        safe_ass = ass_path.replace("\\", "/").replace(":", "\\:")

        cmd2 = [
            "ffmpeg", "-y",
            "-i", raw_path,
            "-vf", f"ass={safe_ass}",
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
            "-c:a", "copy", "-threads", "2",
            output_path,
        ]
        r2 = subprocess.run(cmd2, capture_output=True, text=True)
        if r2.returncode != 0:
            # Caption burn failed — fall back to raw clip so user still gets something
            print(f"  [captions] Burn failed, using raw clip: {r2.stderr[-200:]}")
            os.rename(raw_path, output_path)
        else:
            os.remove(raw_path)

        # Clean up .ass file
        try:
            os.remove(ass_path)
        except OSError:
            pass

        return output_path, None

    except Exception as e:
        return output_path, str(e)


def create_clips(video_path, clips_metadata, output_dir="output", captions=True):
    os.makedirs(output_dir, exist_ok=True)

    if not clips_metadata:
        print("No clip metadata returned from analysis.")
        return []

    # Probe total duration
    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", video_path],
        capture_output=True, text=True
    )
    try:
        total_duration = float(probe.stdout.strip())
    except ValueError:
        total_duration = VideoFileClip(video_path).duration

    # Build task list
    tasks, meta_map = [], {}
    for i, metadata in enumerate(clips_metadata):
        start = max(0.0, min(float(metadata.get("start_time", 0)), total_duration))
        end   = max(start + 1.0, min(float(metadata.get("end_time", 10)), total_duration))

        output_filename = f"clip_{i}_{uuid.uuid4().hex[:8]}.mp4"
        output_path     = os.path.join(output_dir, output_filename)
        tasks.append((video_path, start, end, output_path, captions))
        meta_map[output_path] = {
            "filename":    output_filename,
            "description": metadata.get("description", ""),
            "start_time":  start,
            "end_time":    end,
            "captions":    captions,
        }

    n_workers = min(len(tasks), 4)
    print(f"Rendering {len(tasks)} clip(s) in parallel ({n_workers} workers), captions={'on' if captions else 'off'}...")

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

    order = {t[3]: idx for idx, t in enumerate(tasks)}
    created_files.sort(key=lambda c: order.get(os.path.join(output_dir, c["filename"]), 999))
    return created_files