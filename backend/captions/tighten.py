"""
Tighten — find dead air, fillers, and stutters in a transcript.

Everything here is arithmetic over word timings we already have; no new
model runs. Detection returns a list of Cuts which the caller applies,
previews, or ignores.

The filler design is driven by measured evidence, not a word list. In the
project's 757-word reference transcript, every single occurrence of
"toh", "na", "haan", and "yaani" was a real word doing grammatical work
("kahin na kahin", "pehla toh ye ki", "yaani usne thoda context..."), and
every one of them had **no pause on either side**. A naive list-matching
cutter scores 0% precision there — it would delete seven real words.

So words split into two classes:

  * non-lexical ("umm", "uh", "hmm") are not words in any language and are
    cut on sight;
  * lexical fillers ("matlab", "toh", "yaani") are real vocabulary, and are
    only ever cut when the surrounding silence and duration say the speaker
    was stalling rather than speaking.

When in doubt this errs toward keeping audio. A wrongly cut word is a
broken sentence the user has to hunt for; a missed filler is one click.
"""

import re
import subprocess
from dataclasses import dataclass, asdict
from statistics import median

# Never real words — safe to cut wherever they appear.
NONLEXICAL_FILLERS = {
    "um", "umm", "ummm", "uh", "uhh", "uhm", "erm", "er", "hmm", "hmmm",
    "mmm", "mm", "ahh", "uhhh", "eh",
}

# Devanagari spellings of the same non-lexical sounds, for transcripts kept
# in the original script.
# Only sounds with no lexical meaning. Deliberately excludes "आ" (the
# imperative "come") and other single vowels, which are real words.
NONLEXICAL_FILLERS |= {"उम", "उम्म", "हम्म", "हूँ", "अःः"}

# Real vocabulary that is *sometimes* filler. Never cut on spelling alone.
LEXICAL_FILLERS = {
    # Hinglish — the ones English-only tools miss entirely
    "matlab", "yaani", "yani", "toh", "na", "bas", "aisa", "waise",
    "arre", "achha", "acha", "haan", "samjhe", "yaar",
    # English
    # Single tokens only — detection compares one word at a time, so
    # multi-word crutches ("you know", "i mean") would silently never match.
    # They need n-gram matching over adjacent words; listing them here
    # without it would be a lie about what is supported.
    "like", "actually", "basically", "literally", "obviously", "right",
    "so", "well", "okay", "ok",
    # Devanagari forms of the Hinglish entries above
    "मतलब", "यानी", "तो", "ना", "बस", "ऐसा", "वैसे", "अरे", "अच्छा", "हाँ", "हां",
}


# Hindi reduplicates for emphasis or plurality — "alag-alag" (various),
# "dheere-dheere" (gradually). Saying it twice IS the word.
REDUPLICATED = {
    "alag", "dheere", "baar", "jaldi", "thoda", "kabhi", "saath", "door",
    "paas", "kuch", "bahut", "acha", "achha", "chhota", "bada", "garam",
}

CLAUSE_PUNCT = ".,!?;:।"


def _text(word: dict) -> str:
    """
    The written form to match against.

    Prefers the romanized field: the filler lists are Latin, and a Devanagari
    transcript would never match "matlab". Falls back through the other
    fields so any transcript shape works.
    """
    return (word.get("hinglish") or word.get("text")
            or word.get("devanagari") or "")


@dataclass
class Cut:
    """A span of the original timeline proposed for removal."""
    start: float
    end: float
    reason: str            # "silence" | "filler" | "repeat"
    confidence: float      # 0..1
    text: str = ""
    auto: bool = False     # True = safe to apply without asking

    @property
    def duration(self) -> float:
        return max(0.0, self.end - self.start)

    def to_dict(self) -> dict:
        return {**asdict(self), "duration": round(self.duration, 3)}


@dataclass
class TightenConfig:
    # Silence
    silence: bool = True
    min_gap: float = 0.40          # gaps shorter than this are natural speech rhythm
    keep_padding: float = 0.12     # breathing room left at each end of a cut gap

    # Fillers
    fillers: bool = True
    lexical_fillers: bool = True   # allow context-scored real words to be flagged
    filler_auto_threshold: float = 0.70    # cut without asking at or above this
    filler_suggest_threshold: float = 0.45  # below this, don't even mention it
    min_pause_signal: float = 0.25  # a "pause" for scoring purposes

    # Repeats
    repeats: bool = True

    # Level below which audio counts as silence, for silencedetect.
    silence_db: int = -32

    # Applied to every cut so tight edits never clip a consonant. Whisper's
    # word timings drift by roughly ±100ms.
    edge_padding: float = 0.06


def _clean(text: str) -> str:
    return (text or "").strip().strip(".,!?;:\"'—–-").lower()


def _gap_before(words: list, i: int) -> float:
    return words[i]["start"] - words[i - 1]["end"] if i > 0 else 0.0


def _gap_after(words: list, i: int) -> float:
    return words[i + 1]["start"] - words[i]["end"] if i + 1 < len(words) else 0.0


# ── Detection ────────────────────────────────────────────────────────────────

def detect_silences_from_audio(media_path: str, cfg: TightenConfig) -> list:
    """
    True silence, measured from the audio with ffmpeg's silencedetect.

    This exists because inferring silence from word timings does not work:
    Whisper emits back-to-back spans and stretches a word's end time across
    the following pause. On a test clip with a real two-second gap, every
    word-to-word gap was exactly 0.00 and one "word" lasted 2.44s. The audio
    is the only honest source, and one pass over it is cheap.
    """
    cmd = ["ffmpeg", "-i", media_path, "-af",
           f"silencedetect=noise={cfg.silence_db}dB:d={cfg.min_gap:.3f}",
           "-f", "null", "-"]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        # Otherwise an unreadable file parses as "no silence found" and
        # dead-air detection is skipped without anyone noticing.
        raise RuntimeError(f"silencedetect failed: {r.stderr[-300:]}")
    # silencedetect reports its events on stderr
    starts = [float(m) for m in re.findall(r"silence_start: (-?[\d.]+)", r.stderr)]
    ends = [float(m) for m in re.findall(r"silence_end: (-?[\d.]+)", r.stderr)]

    cuts = []
    for i, s in enumerate(starts):
        e = ends[i] if i < len(ends) else None
        if e is None:          # trailing silence to end of file
            continue
        start = s + cfg.keep_padding
        end = e - cfg.keep_padding
        if end - start <= 0.05:
            continue
        gap = e - s
        confidence = min(1.0, 0.6 + (gap - cfg.min_gap) / 2.0)
        cuts.append(Cut(start, end, "silence", round(confidence, 2),
                        text=f"{gap:.2f}s silence", auto=True))
    return cuts


def detect_silences(words: list, cfg: TightenConfig) -> list:
    """
    Silence inferred from gaps between words.

    Only correct for backends that actually leave gaps (faster-whisper with
    VAD does; mlx-whisper does not). Prefer detect_silences_from_audio when
    the media file is available.
    """
    cuts = []
    for i in range(1, len(words)):
        gap = words[i]["start"] - words[i - 1]["end"]
        if gap < cfg.min_gap:
            continue
        start = words[i - 1]["end"] + cfg.keep_padding
        end = words[i]["start"] - cfg.keep_padding
        if end - start <= 0.05:
            continue
        # Longer dead air is more obviously worth removing.
        confidence = min(1.0, 0.6 + (gap - cfg.min_gap) / 2.0)
        cuts.append(Cut(start, end, "silence", round(confidence, 2),
                        text=f"{gap:.2f}s pause", auto=True))
    return cuts


def detect_repeats(words: list) -> list:
    """
    Consecutive identical words — a stutter like "main main". Keep the last.

    Most repeated words in real speech are not stutters, and the reference
    transcript proves it: all eight repeats there were deliberate. Seven were
    rhetorical restarts across a clause boundary ("sabse upar aaya Claude.
    Claude ne...") and one was "alag-alag", where the repetition is the word.
    Each guard below exists because of a real false positive.
    """
    cuts = []
    for i in range(1, len(words)):
        raw_prev, raw_cur = _text(words[i - 1]), _text(words[i])
        prev, cur = _clean(raw_prev), _clean(raw_cur)
        if not prev or prev != cur:
            continue
        # "Claude, Claude ne..." — punctuation means a new clause started,
        # so this is rhetoric, not a stumble.
        if raw_prev.strip() and raw_prev.strip()[-1] in CLAUSE_PUNCT:
            continue
        # "alag -alag" — a hyphen on either side marks reduplication.
        if "-" in raw_prev or "-" in raw_cur:
            continue
        if prev in REDUPLICATED:
            continue
        # A repeat spoken with a pause between is usually deliberate emphasis
        # ("bahut bahut shukriya"), not a stutter.
        if _gap_before(words, i) > 0.35:
            continue
        cuts.append(Cut(words[i - 1]["start"], words[i - 1]["end"], "repeat",
                        0.8, text=_text(words[i - 1]), auto=True))
    return cuts


def _filler_score(words: list, i: int, cfg: TightenConfig,
                  token_durations: dict) -> float:
    """
    How much does this occurrence behave like stalling rather than speech?

    Pauses on both sides are the strongest signal — that is what separated
    every real "toh" in our reference transcript (no pauses) from an actual
    filler (surrounded by dead air).
    """
    w = words[i]
    token = _clean(_text(w))
    gap_b, gap_a = _gap_before(words, i), _gap_after(words, i)
    duration = w["end"] - w["start"]

    score = 0.0
    if gap_b >= cfg.min_pause_signal:
        score += 0.35
    if gap_a >= cfg.min_pause_signal:
        score += 0.35

    # Fillers get drawn out: "maaatlab". Compare against this speaker's own
    # typical delivery of the same token.
    typical = token_durations.get(token)
    if typical and duration >= typical * 1.6:
        score += 0.20

    # Someone leaning on a crutch word says it a lot in a short window.
    window = [x for x in words
              if abs(x["start"] - w["start"]) <= 15.0 and _clean(_text(x)) == token]
    if len(window) >= 4:
        score += 0.15

    return min(1.0, score)


def detect_fillers(words: list, cfg: TightenConfig) -> list:
    """Non-lexical fillers always; real words only when context agrees."""
    cuts = []

    by_token: dict = {}
    for w in words:
        by_token.setdefault(_clean(_text(w)), []).append(w["end"] - w["start"])
    token_durations = {t: median(d) for t, d in by_token.items() if len(d) >= 2}

    for i, w in enumerate(words):
        token = _clean(_text(w))
        if not token:
            continue

        if token in NONLEXICAL_FILLERS:
            cuts.append(Cut(w["start"], w["end"], "filler", 1.0,
                            text=_text(w), auto=True))
            continue

        if not cfg.lexical_fillers or token not in LEXICAL_FILLERS:
            continue

        score = _filler_score(words, i, cfg, token_durations)
        if score < cfg.filler_suggest_threshold:
            continue
        cuts.append(Cut(w["start"], w["end"], "filler", round(score, 2),
                        text=_text(w), auto=score >= cfg.filler_auto_threshold))
    return cuts


def _merge(cuts: list, edge_padding: float, duration: float | None) -> list:
    """
    Sort, pad, and merge overlapping cuts.

    Auto cuts and suggestions are merged separately. Merging them together
    would let a low-confidence suggestion swallow a safe silence cut and
    demote the whole span to "ask first", so applying auto cuts would
    silently drop it.
    """
    if not cuts:
        return []
    if any(c.auto for c in cuts) and any(not c.auto for c in cuts):
        return sorted(
            _merge([c for c in cuts if c.auto], edge_padding, duration)
            + _merge([c for c in cuts if not c.auto], edge_padding, duration),
            key=lambda c: c.start)
    padded = []
    for c in sorted(cuts, key=lambda c: c.start):
        start = max(0.0, c.start - edge_padding)
        end = c.end + edge_padding
        if duration is not None:
            end = min(end, duration)
        if end > start:
            padded.append(Cut(start, end, c.reason, c.confidence, c.text, c.auto))

    merged = [padded[0]]
    for c in padded[1:]:
        last = merged[-1]
        if c.start <= last.end:
            # Overlapping spans become one; keep the stronger claim.
            last.end = max(last.end, c.end)
            if c.confidence > last.confidence:
                last.reason, last.confidence, last.text = c.reason, c.confidence, c.text
        else:
            merged.append(c)
    return merged


def detect(words: list, cfg: TightenConfig | None = None,
           duration: float | None = None, media_path: str | None = None) -> list:
    """
    Transcript in, proposed cuts out. Nothing is applied here.

    Pass media_path to measure silence from the audio, which is the only
    reliable way — see detect_silences_from_audio.
    """
    cfg = cfg or TightenConfig()
    if not words:
        return []
    cuts = []
    if cfg.silence:
        cuts += (detect_silences_from_audio(media_path, cfg) if media_path
                 else detect_silences(words, cfg))
    if cfg.fillers:
        cuts += detect_fillers(words, cfg)
    if cfg.repeats:
        cuts += detect_repeats(words)
    return _merge(cuts, cfg.edge_padding, duration)


# ── Applying ─────────────────────────────────────────────────────────────────

def keep_segments(cuts: list, duration: float) -> list:
    """Invert the cut list into the spans that survive: [(start, end), ...]."""
    kept, pos = [], 0.0
    for c in sorted(cuts, key=lambda c: c.start):
        if c.start > pos:
            kept.append((pos, min(c.start, duration)))
        pos = max(pos, c.end)
    if pos < duration:
        kept.append((pos, duration))
    return [(s, e) for s, e in kept if e - s > 0.01]


def apply_cuts(words: list, cuts: list, duration: float) -> dict:
    """
    Re-time a transcript onto the cut timeline.

    This is the whole reason cutting belongs next to captioning: remove a
    span and every later word shifts earlier, so captions produced anywhere
    else would need redoing. Returns the surviving words with corrected
    timings plus the new total duration.
    """
    kept = keep_segments(cuts, duration)
    if not kept:
        return {"words": [], "duration": 0.0, "kept": []}

    # offset[i] = how much time precedes segment i on the new timeline
    offsets, running = [], 0.0
    for s, e in kept:
        offsets.append(running)
        running += e - s

    out = []
    for w in words:
        # Assign by largest overlap, not by midpoint. Whisper stretches a
        # word's end time across the pause that follows it, so a real word
        # can have its midpoint land inside a silence cut — midpoint
        # assignment silently deleted "bhali" from a test clip's captions.
        best_idx, best_overlap = None, 0.0
        zero_length = w["end"] <= w["start"]
        for idx, (s, e) in enumerate(kept):
            overlap = min(w["end"], e) - max(w["start"], s)
            # A zero-length word can never win on overlap, so fall back to
            # containment — otherwise real words with identical start/end
            # timestamps disappear from the captions.
            if zero_length and s <= w["start"] <= e:
                overlap = max(overlap, 1e-6)
            if overlap > best_overlap:
                best_idx, best_overlap = idx, overlap
        if best_idx is None:
            continue        # genuinely inside a cut — it was silence or filler

        s, e = kept[best_idx]
        new_start = offsets[best_idx] + max(0.0, w["start"] - s)
        new_end = offsets[best_idx] + min(e - s, w["end"] - s)
        if new_end <= new_start:
            new_end = new_start + 0.08     # keep clipped words visible
        out.append({**w, "start": round(new_start, 3), "end": round(new_end, 3)})

    return {"words": out, "duration": round(running, 3), "kept": kept}


def summarize(cuts: list, duration: float) -> dict:
    """Numbers for the UI: what was found, and what it buys."""
    applied = [c for c in cuts if c.auto]
    by_reason: dict = {}
    for c in cuts:
        r = by_reason.setdefault(c.reason, {"count": 0, "seconds": 0.0})
        r["count"] += 1
        r["seconds"] = round(r["seconds"] + c.duration, 2)
    removed = round(sum(c.duration for c in applied), 2)
    return {
        "original_duration": round(duration, 2),
        "tightened_duration": round(duration - removed, 2),
        "removed_seconds": removed,
        "removed_percent": round(removed / duration * 100, 1) if duration else 0.0,
        "auto_cuts": len(applied),
        "suggested_cuts": len(cuts) - len(applied),
        "by_reason": by_reason,
    }
