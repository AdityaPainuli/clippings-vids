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
import time as _time
from concurrent.futures import ProcessPoolExecutor, as_completed

from dotenv import load_dotenv

load_dotenv()


GENAI_API_KEY = os.getenv("GOOGLE_API_KEY")
if GENAI_API_KEY:
    genai.configure(api_key=GENAI_API_KEY)

# ---------------------------------------------------------------------------
# Clip style definitions — user picks one to bias Gemini's selection
# ---------------------------------------------------------------------------
CLIP_STYLES = {
    "auto":          "Automatically detect the best clip style based on content",
    "funny":         "Prioritize humor, punchlines, comedic timing, and laugh-out-loud moments",
    "educational":   "Focus on insights, aha-moments, surprising facts, and clear explanations",
    "emotional":     "Find emotionally powerful moments — vulnerability, triumph, heartbreak, inspiration",
    "controversial": "Surface bold opinions, hot takes, disagreements, and debate-worthy statements",
    "highlights":    "Extract the most action-packed, visually striking, peak moments",
}

# ---------------------------------------------------------------------------
# Caption style presets — user picks one; values feed into ASS subtitle gen
# ---------------------------------------------------------------------------
CAPTION_PRESETS = {
    "default": {
        "font": os.getenv("CAPTION_FONT", "Arial Black"),
        "fontsize": int(os.getenv("CAPTION_FONTSIZE", "72")),
        "color": os.getenv("CAPTION_COLOR", "&H00FFFFFF"),
        "highlight": os.getenv("CAPTION_HIGHLIGHT", "&H0000FFFF"),
        "outline_clr": os.getenv("CAPTION_OUTLINE_CLR", "&H00000000"),
        "shadow_clr": os.getenv("CAPTION_SHADOW_CLR", "&H66000000"),
        "outline_w": int(os.getenv("CAPTION_OUTLINE_W", "4")),
        "shadow_w": int(os.getenv("CAPTION_SHADOW_W", "3")),
        "words_per_line": int(os.getenv("CAPTION_WORDS", "3")),
        "margin_v": int(os.getenv("CAPTION_MARGIN_V", "320")),
        "anim_type": "none",
    },
    "bold_impact": {
        "font": "Arial Black",
        "fontsize": 80,
        "color": "&H00FFFFFF",
        "highlight": "&H0000FFFF",
        "outline_clr": "&H00000000",
        "shadow_clr": "&H66000000",
        "outline_w": 5,
        "shadow_w": 3,
        "words_per_line": 3,
        "margin_v": 320,
        "anim_type": "pop",
        "anim_scale_start": 50,
        "anim_scale_end": 115,
        "anim_duration_ms": 150,
    },
    "subtle": {
        "font": "Inter",
        "fontsize": 56,
        "color": "&H00FFFFFF",
        "highlight": "&H0088FF88",
        "outline_clr": "&H00000000",
        "shadow_clr": "&H44000000",
        "outline_w": 2,
        "shadow_w": 1,
        "words_per_line": 4,
        "margin_v": 300,
        "anim_type": "fade",
        "anim_duration_ms": 200,
    },
    "karaoke": {
        "font": "Arial Black",
        "fontsize": 72,
        "color": "&H00FFFFFF",
        "highlight": "&H000055FF",
        "outline_clr": "&H00000000",
        "shadow_clr": "&H66000000",
        "outline_w": 4,
        "shadow_w": 2,
        "words_per_line": 5,
        "margin_v": 320,
        "anim_type": "karaoke",
    },
}

# Backward-compat module-level constants (used by legacy code paths)
CAPTION_FONT         = CAPTION_PRESETS["default"]["font"]
CAPTION_FONTSIZE     = CAPTION_PRESETS["default"]["fontsize"]
CAPTION_COLOR        = CAPTION_PRESETS["default"]["color"]
CAPTION_HIGHLIGHT    = CAPTION_PRESETS["default"]["highlight"]
CAPTION_OUTLINE_CLR  = CAPTION_PRESETS["default"]["outline_clr"]
CAPTION_SHADOW_CLR   = CAPTION_PRESETS["default"]["shadow_clr"]
CAPTION_OUTLINE_W    = CAPTION_PRESETS["default"]["outline_w"]
CAPTION_SHADOW_W     = CAPTION_PRESETS["default"]["shadow_w"]
CAPTION_WORDS        = CAPTION_PRESETS["default"]["words_per_line"]
CAPTION_MARGIN_V     = CAPTION_PRESETS["default"]["margin_v"]


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


def _retry(fn, max_attempts=3, backoff_base=2.0):
    """Retry a callable with exponential backoff. Returns the first successful result."""
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            last_error = e
            if attempt < max_attempts:
                wait = backoff_base ** (attempt - 1)
                print(f"  [retry] Attempt {attempt}/{max_attempts} failed: {e}. "
                      f"Retrying in {wait:.1f}s...")
                _time.sleep(wait)
    raise last_error


def analyze_video(video_path, user_instructions=None, info=None, clip_style="auto"):
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

    # ── 4b. Clip style directive ──────────────────────────────────────────────
    style_directive = ""
    if clip_style and clip_style != "auto" and clip_style in CLIP_STYLES:
        style_directive = (
            f"\n\nCLIP STYLE PRIORITY (this overrides content-type defaults):\n"
            f"The user specifically wants: {clip_style.upper()}\n"
            f"Directive: {CLIP_STYLES[clip_style]}\n"
            f"Every clip you return MUST strongly align with this style. "
            f"Discard moments that don't match even if they have high general virality."
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
5. QUOTABILITY — prioritise moments with a line someone would screenshot or repeat.{transcript_section}{custom_focus}{style_directive}

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

    # Call Gemini with retry (handles transient network/rate-limit errors)
    print("Calling Gemini for viral moment analysis...")

    def _call_gemini():
        model = genai.GenerativeModel(model_name="gemini-2.5-pro")
        return model.generate_content(content_parts)

    response = _retry(_call_gemini, max_attempts=3, backoff_base=2.0)

    try:
        clips = _parse_gemini_json(response.text)
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


def _get_preset(preset_name: str) -> dict:
    """Look up a caption preset by name, falling back to default."""
    return CAPTION_PRESETS.get(preset_name, CAPTION_PRESETS["default"])


def _build_ass_header(preset_name: str = "default") -> str:
    """
    Two styles:
      Caption   – static words, thick black outline + drop shadow
      Highlight – active word, slightly larger
    BorderStyle 1 = outline+shadow (cleaner than opaque box).
    Alignment 2 = bottom-center.
    """
    p = _get_preset(preset_name)

    outline_w = str(p["outline_w"])
    shadow_w  = str(p["shadow_w"])
    margin_v  = str(p["margin_v"])
    size      = str(p["fontsize"])
    big_size  = str(int(p["fontsize"] * 1.12))
    font      = p["font"]

    fmt = ("Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
           "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
           "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
           "Alignment, MarginL, MarginR, MarginV, Encoding\n")

    tail = "-1,0,0,0,100,100,2,0,1," + outline_w + "," + shadow_w + ",2,40,40," + margin_v + ",1\n"

    caption_style   = ("Style: Caption,"   + font + "," + size     + ","
                       + p["color"]       + ",&H000000FF,"
                       + p["outline_clr"] + "," + p["shadow_clr"] + "," + tail)
    highlight_style = ("Style: Highlight," + font + "," + big_size + ","
                       + p["highlight"]   + ",&H000000FF,"
                       + p["outline_clr"] + "," + p["shadow_clr"] + "," + tail)

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


def _anim_tag_pop(colour, alpha, scx_start, scx_end, settle_ms, overshoot_ms):
    """Build an ASS override with \\t transform for pop-in animation."""
    # Scale from scx_start → scx_end (overshoot) then settle to 100
    return ("{" +
            "\\c" + colour +
            "\\alpha" + alpha +
            "\\fscx" + str(scx_start) + "\\fscy" + str(scx_start) +
            "\\t(0," + str(overshoot_ms) + ",\\fscx" + str(scx_end) + "\\fscy" + str(scx_end) + ")" +
            "\\t(" + str(overshoot_ms) + "," + str(overshoot_ms + settle_ms) + ",\\fscx100\\fscy100)" +
            "}")


def _anim_tag_fade(colour, alpha, duration_ms):
    """Build an ASS override with fade-in animation."""
    return ("{" +
            "\\c" + colour +
            "\\alpha&HFF" +
            "\\t(0," + str(duration_ms) + ",\\alpha" + alpha + ")" +
            "}")


def _words_to_ass_events(words: list, preset_name: str = "default") -> str:
    """
    One Dialogue line per word in each N-word chunk.
    Supports multiple animation styles based on the caption preset:
      - none (default): static highlight (past=white, active=yellow+bigger, future=dimmed)
      - pop (bold_impact): active word pops in with scale overshoot
      - fade (subtle): words fade in smoothly
      - karaoke: uses ASS \\kf progressive fill
    """
    if not words:
        return ""

    p = _get_preset(preset_name)
    words_per_line = p.get("words_per_line", CAPTION_WORDS)
    anim_type      = p.get("anim_type", "none")

    FULL_ALPHA = "&H00"
    DIM_ALPHA  = "&HAA"
    WHITE      = p.get("color", "&H00FFFFFF")
    HIGHLIGHT  = p.get("highlight", "&H0000FFFF")

    event_lines = []

    for i in range(0, len(words), words_per_line):
        chunk       = words[i : i + words_per_line]
        chunk_end   = chunk[-1]["end"]
        word_texts  = [w["text"].upper().replace("{", "").replace("}", "") for w in chunk]

        # ── Karaoke mode: single line per chunk with \kf tags ────────────
        if anim_type == "karaoke":
            seg_start = chunk[0]["start"]
            parts = []
            for w_idx, w in enumerate(chunk):
                dur_cs = int((w["end"] - w["start"]) * 100)  # centiseconds for \kf
                dur_cs = max(dur_cs, 10)
                parts.append("{\\kf" + str(dur_cs) + "}" + word_texts[w_idx])
            line_text = " ".join(parts)
            event_lines.append(
                "Dialogue: 0,"
                + _seconds_to_ass_time(seg_start) + ","
                + _seconds_to_ass_time(chunk_end)
                + ",Caption,,0,0,0,," + line_text
            )
            continue

        # ── Pop / Fade / None: one Dialogue per active word ──────────────
        for active_idx, active_word in enumerate(chunk):
            seg_start = active_word["start"]
            seg_end   = (chunk[active_idx + 1]["start"]
                         if active_idx + 1 < len(chunk) else chunk_end)
            if seg_end <= seg_start:
                seg_end = seg_start + 0.1

            parts = []
            for j, wt in enumerate(word_texts):
                if j < active_idx:
                    # Past words — fully visible, base color
                    parts.append(_tag(WHITE, FULL_ALPHA) + wt)
                elif j == active_idx:
                    # Active word — animated based on preset
                    if anim_type == "pop":
                        sc_start = p.get("anim_scale_start", 50)
                        sc_end   = p.get("anim_scale_end", 115)
                        dur_ms   = p.get("anim_duration_ms", 150)
                        parts.append(
                            _anim_tag_pop(HIGHLIGHT, FULL_ALPHA, sc_start, sc_end, 100, dur_ms)
                            + wt
                            + _tag(WHITE, FULL_ALPHA, "100", "100")
                        )
                    elif anim_type == "fade":
                        dur_ms = p.get("anim_duration_ms", 200)
                        parts.append(
                            _anim_tag_fade(HIGHLIGHT, FULL_ALPHA, dur_ms)
                            + wt
                            + _tag(WHITE, FULL_ALPHA, "100", "100")
                        )
                    else:
                        # Default static highlight
                        parts.append(_tag(HIGHLIGHT, FULL_ALPHA, "115", "115") + wt
                                     + _tag(WHITE, FULL_ALPHA, "100", "100"))
                else:
                    # Future words — dimmed
                    parts.append(_tag(WHITE, DIM_ALPHA) + wt)

            line_text = (" ".join(parts)
                         + _tag(WHITE, FULL_ALPHA))

            event_lines.append(
                "Dialogue: 0,"
                + _seconds_to_ass_time(seg_start) + ","
                + _seconds_to_ass_time(seg_end)
                + ",Caption,,0,0,0,," + line_text
            )

    return "\n".join(event_lines)


def generate_captions_ass(video_path: str, clip_duration: float, output_ass: str,
                          caption_style: str = "default") -> bool:
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

    ass_content = _build_ass_header(caption_style) + _words_to_ass_events(words, caption_style)
    with open(output_ass, "w", encoding="utf-8") as f:
        f.write(ass_content)

    print(f"  [captions] {len(words)} word(s) written to {os.path.basename(output_ass)} (style={caption_style})")
    return True

# ---------------------------------------------------------------------------
# Clip rendering — parallel, blur-pad background, burned captions
# ---------------------------------------------------------------------------

def _detect_face_region(video_path, start, end, sample_interval=2.0):
    """
    Sample frames and detect faces using OpenCV.
    Returns average face center (x_frac, y_frac) relative to frame size,
    or None if no faces found or OpenCV is unavailable.
    """
    try:
        import cv2
    except ImportError:
        return None

    cascade_path = os.path.join(cv2.data.haarcascades, "haarcascade_frontalface_default.xml")
    if not os.path.exists(cascade_path):
        return None
    face_cascade = cv2.CascadeClassifier(cascade_path)

    duration = end - start
    n_samples = max(1, int(duration / sample_interval))
    face_centers = []

    with tempfile.TemporaryDirectory() as tmpdir:
        for i in range(n_samples):
            ts = start + (i + 0.5) * (duration / n_samples)
            frame_path = os.path.join(tmpdir, f"face_{i}.jpg")
            subprocess.run(
                ["ffmpeg", "-ss", str(ts), "-i", video_path,
                 "-frames:v", "1", "-q:v", "8", frame_path, "-y"],
                capture_output=True
            )
            if not os.path.exists(frame_path):
                continue

            img = cv2.imread(frame_path)
            if img is None:
                continue
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(60, 60))

            h, w = img.shape[:2]
            for (fx, fy, fw, fh) in faces:
                cx = (fx + fw / 2) / w
                cy = (fy + fh / 2) / h
                face_centers.append((cx, cy))

    if not face_centers:
        return None

    avg_x = sum(c[0] for c in face_centers) / len(face_centers)
    avg_y = sum(c[1] for c in face_centers) / len(face_centers)
    return (avg_x, avg_y)


def _build_vf_with_face(face_center):
    """
    Build FFmpeg video filter that crops toward the detected face
    and uses a blurred background — same structure as the default
    filter but with face-biased positioning instead of center.
    """
    fx, fy = face_center
    fx = max(0.25, min(0.75, fx))
    fy = max(0.25, min(0.75, fy))

    # Calculate crop offset for the foreground: shift toward face
    # (ow-iw)/2 is center; we bias it by the face offset
    x_offset = f"(ow-iw)/2+{int((fx - 0.5) * 200)}"
    y_offset = f"(oh-ih)/2+{int((fy - 0.5) * 200)}"

    vf = (
        "[0:v]split=2[bg][fg];"
        "[bg]scale=1080:1920:force_original_aspect_ratio=increase,"
        "crop=1080:1920,"
        "gblur=sigma=40,"
        "eq=brightness=-0.3[bg_blurred];"
        f"[fg]scale=1080:1920:force_original_aspect_ratio=decrease,"
        f"pad=1080:1920:{x_offset}:{y_offset}:color=black@0[fg_scaled];"
        "[bg_blurred][fg_scaled]overlay=0:0"
    )
    return vf


def _render_single_clip(args):
    """
    Worker process:
      1. Optionally detect faces for smart cropping
      2. Render vertical 9:16 clip with blurred background (ffmpeg)
      3. Transcribe audio with Whisper → write .ass subtitle file
      4. Burn captions into the clip (second ffmpeg pass)
    """
    video_path, start, end, output_path, captions_enabled, caption_style = args
    try:
        duration = end - start

        # ── Face detection (optional, best-effort) ────────────────────────────
        face_center = None
        try:
            face_center = _detect_face_region(video_path, start, end)
            if face_center:
                print(f"  [face] Detected face at ({face_center[0]:.2f}, {face_center[1]:.2f})")
        except Exception:
            pass  # Fall back to center-crop silently

        # ── Pass 1: vertical conversion ───────────────────────────────────────
        raw_path = output_path.replace(".mp4", "_raw.mp4")

        if face_center:
            # Face-biased crop with blurred background
            vf = _build_vf_with_face(face_center)
        else:
            # Standard blur-pad background
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
            # If face-zoom filter failed, retry with standard blur-pad
            if face_center:
                print(f"  [face] Zoom filter failed, falling back to blur-pad")
                vf_fallback = (
                    "[0:v]split=2[bg][fg];"
                    "[bg]scale=1080:1920:force_original_aspect_ratio=increase,"
                    "crop=1080:1920,"
                    "gblur=sigma=40,"
                    "eq=brightness=-0.3[bg_blurred];"
                    "[fg]scale=1080:1920:force_original_aspect_ratio=decrease,"
                    "pad=1080:1920:(ow-iw)/2:(oh-ih)/2:color=black@0[fg_scaled];"
                    "[bg_blurred][fg_scaled]overlay=0:0"
                )
                cmd1_retry = [
                    "ffmpeg", "-y",
                    "-ss", str(start), "-i", video_path,
                    "-t", str(duration),
                    "-vf", vf_fallback,
                    "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
                    "-c:a", "aac", "-threads", "2",
                    raw_path,
                ]
                r1 = subprocess.run(cmd1_retry, capture_output=True, text=True)
                if r1.returncode != 0:
                    raise RuntimeError(f"Pass 1 failed: {r1.stderr[-400:]}")
            else:
                raise RuntimeError(f"Pass 1 failed: {r1.stderr[-400:]}")

        if not captions_enabled:
            os.rename(raw_path, output_path)
            return output_path, None

        # ── Pass 2: burn captions ─────────────────────────────────────────────
        ass_path = output_path.replace(".mp4", ".ass")
        has_captions = generate_captions_ass(raw_path, duration, ass_path, caption_style)

        if not has_captions:
            os.rename(raw_path, output_path)
            return output_path, None

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
            print(f"  [captions] Burn failed, using raw clip: {r2.stderr[-200:]}")
            os.rename(raw_path, output_path)
        else:
            os.remove(raw_path)

        try:
            os.remove(ass_path)
        except OSError:
            pass

        return output_path, None

    except Exception as e:
        return output_path, str(e)


MAX_RENDER_ATTEMPTS = 2


def create_clips(video_path, clips_metadata, output_dir="output", captions=True,
                  caption_style="default"):
    """
    Render clips in parallel. Returns (created_files, failed_clips) tuple.
    Partial results are returned even if some clips fail.
    """
    os.makedirs(output_dir, exist_ok=True)

    if not clips_metadata:
        print("No clip metadata returned from analysis.")
        return [], []

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
        tasks.append((video_path, start, end, output_path, captions, caption_style))
        meta_map[output_path] = {
            "filename":       output_filename,
            "description":    metadata.get("description", ""),
            "start_time":     start,
            "end_time":       end,
            "hook":           metadata.get("hook", ""),
            "virality_score": metadata.get("virality_score", 0),
            "clip_type":      metadata.get("clip_type", ""),
            "captions":       captions,
        }

    n_workers = min(len(tasks), 4)
    print(f"Rendering {len(tasks)} clip(s) in parallel ({n_workers} workers), "
          f"captions={'on' if captions else 'off'}, caption_style={caption_style}...")

    created_files = []
    failed_clips = []

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        # First pass
        futures = {executor.submit(_render_single_clip, t): t for t in tasks}
        retry_tasks = []

        for future in as_completed(futures):
            output_path, err = future.result()
            if err:
                original_task = futures[future]
                retry_tasks.append(original_task)
                print(f"  x Failed {os.path.basename(output_path)} (attempt 1): {err}")
            else:
                print(f"  + Done:   {os.path.basename(output_path)}")
                created_files.append(meta_map[output_path])

        # Retry failed clips once
        if retry_tasks:
            print(f"  Retrying {len(retry_tasks)} failed clip(s)...")
            retry_futures = {executor.submit(_render_single_clip, t): t for t in retry_tasks}
            for future in as_completed(retry_futures):
                output_path, err = future.result()
                if err:
                    task = retry_futures[future]
                    failed_clips.append({
                        "filename":   os.path.basename(task[3]),
                        "error":      err,
                        "start_time": task[1],
                        "end_time":   task[2],
                    })
                    print(f"  x Failed {os.path.basename(output_path)} (attempt 2, giving up): {err}")
                else:
                    print(f"  + Done (retry): {os.path.basename(output_path)}")
                    created_files.append(meta_map[output_path])

    order = {t[3]: idx for idx, t in enumerate(tasks)}
    created_files.sort(key=lambda c: order.get(os.path.join(output_dir, c["filename"]), 999))
    return created_files, failed_clips