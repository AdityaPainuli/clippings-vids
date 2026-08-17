"""
ASS subtitle generation from word timings + a CaptionStyle.

Generalized from clipper.py's caption builder: resolution-aware PlayRes,
hex-color styles, and all four animation modes (none/pop/fade/karaoke).
"""

from .styles import CaptionStyle

FULL_ALPHA = "&H00"


def _ass_time(s: float) -> str:
    h = int(s // 3600)
    m = int((s % 3600) // 60)
    sc = s % 60
    return f"{h}:{m:02d}:{sc:05.2f}"


def _tag(colour: str, alpha: str, scx: str = "100", scy: str = "100") -> str:
    return "{" + "\\c" + colour + "\\alpha" + alpha + "\\fscx" + scx + "\\fscy" + scy + "}"


def _pop_tag(colour, alpha, sc_start, sc_end, settle_ms, overshoot_ms):
    return ("{" + "\\c" + colour + "\\alpha" + alpha
            + "\\fscx" + str(sc_start) + "\\fscy" + str(sc_start)
            + "\\t(0," + str(overshoot_ms) + ",\\fscx" + str(sc_end) + "\\fscy" + str(sc_end) + ")"
            + "\\t(" + str(overshoot_ms) + "," + str(overshoot_ms + settle_ms) + ",\\fscx100\\fscy100)"
            + "}")


def _fade_tag(colour, alpha, duration_ms):
    return ("{" + "\\c" + colour + "\\alpha&HFF"
            + "\\t(0," + str(duration_ms) + ",\\alpha" + alpha + ")" + "}")


def _header(style: CaptionStyle, play_w: int, play_h: int) -> str:
    big_size = int(style.font_size * style.highlight_scale)
    fmt = ("Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
           "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
           "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
           "Alignment, MarginL, MarginR, MarginV, Encoding\n")
    tail = (f"-1,0,0,0,100,100,2,0,1,{style.outline_width},{style.shadow_width},"
            f"{style.alignment},40,40,{style.margin_v},1\n")

    caption = (f"Style: Caption,{style.font},{style.font_size},"
               f"{style.ass_text_color},&H000000FF,"
               f"{style.ass_outline_color},{style.ass_shadow_color},{tail}")
    highlight = (f"Style: Highlight,{style.font},{big_size},"
                 f"{style.ass_highlight_color},&H000000FF,"
                 f"{style.ass_outline_color},{style.ass_shadow_color},{tail}")

    return ("[Script Info]\n"
            "ScriptType: v4.00+\n"
            f"PlayResX: {play_w}\n"
            f"PlayResY: {play_h}\n"
            "WrapStyle: 1\n\n"
            "[V4+ Styles]\n"
            + fmt + caption + highlight
            + "\n[Events]\n"
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n")


def _events(words: list, style: CaptionStyle) -> str:
    if not words:
        return ""

    anim = style.animation
    white = style.ass_text_color
    hilite = style.ass_highlight_color
    dim = style.ass_dim_alpha
    lines = []

    for i in range(0, len(words), style.words_per_line):
        chunk = words[i : i + style.words_per_line]
        chunk_end = chunk[-1]["end"]
        texts = [w["text"].replace("{", "").replace("}", "") for w in chunk]
        if style.uppercase:
            texts = [t.upper() for t in texts]

        if anim.type == "karaoke":
            parts = []
            for w_idx, w in enumerate(chunk):
                dur_cs = max(int((w["end"] - w["start"]) * 100), 10)
                parts.append("{\\kf" + str(dur_cs) + "}" + texts[w_idx])
            lines.append(
                f"Dialogue: 0,{_ass_time(chunk[0]['start'])},{_ass_time(chunk_end)}"
                f",Caption,,0,0,0,," + " ".join(parts)
            )
            continue

        for active_idx, active in enumerate(chunk):
            seg_start = active["start"]
            seg_end = (chunk[active_idx + 1]["start"]
                       if active_idx + 1 < len(chunk) else chunk_end)
            if seg_end <= seg_start:
                seg_end = seg_start + 0.1

            parts = []
            for j, wt in enumerate(texts):
                if j < active_idx:
                    parts.append(_tag(white, FULL_ALPHA) + wt)
                elif j == active_idx:
                    if anim.type == "pop":
                        # duration_ms is the total pop: 60% overshoot, 40% settle
                        overshoot_ms = max(1, int(anim.duration_ms * 0.6))
                        settle_ms = max(1, anim.duration_ms - overshoot_ms)
                        parts.append(
                            _pop_tag(hilite, FULL_ALPHA, anim.scale_start,
                                     anim.scale_end, settle_ms, overshoot_ms)
                            + wt + _tag(white, FULL_ALPHA)
                        )
                    elif anim.type == "fade":
                        parts.append(
                            _fade_tag(hilite, FULL_ALPHA, anim.duration_ms)
                            + wt + _tag(white, FULL_ALPHA)
                        )
                    else:
                        scale = str(int(style.highlight_scale * 100))
                        parts.append(_tag(hilite, FULL_ALPHA, scale, scale) + wt
                                     + _tag(white, FULL_ALPHA))
                else:
                    parts.append(_tag(white, dim) + wt)

            lines.append(
                f"Dialogue: 0,{_ass_time(seg_start)},{_ass_time(seg_end)}"
                f",Caption,,0,0,0,," + " ".join(parts) + _tag(white, FULL_ALPHA)
            )

    return "\n".join(lines)


def build_ass(words: list, style: CaptionStyle, play_w: int = 1080, play_h: int = 1920,
              text_key: str = "text") -> str:
    """
    words: [{"start", "end", "text", ...}]. text_key picks which field to
    display ("text" or "hinglish"), so one transcript serves both scripts.
    """
    display = [
        {"start": w["start"], "end": w["end"], "text": w.get(text_key) or w.get("text") or ""}
        for w in words
        if (w.get(text_key) or w.get("text") or "").strip()
    ]
    return _header(style, play_w, play_h) + _events(display, style)
