"""
Find retakes — the same line delivered more than once — and pick the keeper.

This is the single biggest time sink in talking-head editing, and the repeat
detector in tighten.py cannot touch it: that one compares adjacent words, while
a retake runs 5 to 30 words and is rarely word-identical. "so the thing about
compound interest is" and "the thing with compound interest, right, is that"
share no exact repeat at all.

Three stages, and the model only does the middle one:

  1. **Candidates, no model.** Split the transcript into phrases at real
     pauses, then score each phrase against the recent ones with cheap lexical
     overlap. The threshold is deliberately loose — this stage exists to find
     everything worth asking about, not to be right.
  2. **The model adjudicates a group.** All attempts at one line go up in a
     single call, which answers whether they really are retakes and which one
     to keep. It classifies; it never generates. Timestamps and spans come from
     stage 1, so a hallucination cannot invent a cut inside a good sentence.
  3. **Nothing auto-applies.** A filler cut is 300ms; a retake cut is eight
     seconds of speech, and a wrong one destroys the take. Every retake arrives
     as a suggestion with both versions shown.

Handing a whole 40-minute transcript to a model and asking it to find the
retakes would be expensive, unreproducible, and worse on longer files. Stage 1
turns that into a handful of small, focused questions.
"""

import re
from dataclasses import dataclass

from .tighten import CLAUSE_PUNCT, Cut, _text

# A phrase boundary needs a real pause. Whisper does not emit one: it stretches
# each word's end time to the start of the next, so inter-word gaps read 0.00
# even across two seconds of silence (the same trap that moved silence
# detection onto ffmpeg). Real pauses come from the audio; punctuation is the
# fallback when no media is available.
MIN_PAUSE = 0.45

# How far back a restart can reach. A speaker returning to a point five minutes
# later is making that point again, not fixing a flub.
WINDOW = 45.0

# Compare against a bounded number of recent phrases so cost stays flat on a
# long recording.
LOOKBACK = 8

# Cost ceiling per run. Whatever this drops is reported, never swallowed.
DEFAULT_MAX_GROUPS = 12

MIN_WORDS = 6           # shorter phrases match each other by accident
# A re-recorded line is a line. Anything this long is a transcription artifact
# — the punctuation fallback can run 150 words together when Whisper emits no
# punctuation — and comparing it to a short phrase scores high for free.
MAX_WORDS = 40
MIN_SIMILARITY = 0.35   # loose on purpose — stage 2 is the precision stage

# Overlap over the smaller set rewards short phrases: two shared words out of
# five reads as 0.40. These floors demand the match be real before the ratio is
# allowed to speak.
MIN_SHARED_WORDS = 3
MIN_SHARED_BIGRAMS = 1

# Two takes of one line run to roughly the same length. A two-second phrase and
# a twenty-second one are not two attempts at the same thing.
MIN_DURATION_RATIO = 0.4

_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)


@dataclass
class Phrase:
    start: float
    end: float
    i0: int             # word range [i0, i1)
    i1: int
    text: str
    tokens: tuple

    @property
    def duration(self) -> float:
        return max(0.0, self.end - self.start)


def _norm(word: dict) -> str:
    return _PUNCT_RE.sub("", _text(word).lower()).strip()


def split_phrases(words: list, silences: list | None = None) -> list:
    """
    Break the transcript where the speaker actually stopped.

    `silences` is [(start, end), ...] from ffmpeg. Passing None means nothing
    was measured, and clause punctuation stands in — worse, but the only signal
    a bare transcript carries. Passing an empty list means the audio *was*
    measured and holds no qualifying pause, which is an answer: the speaker did
    not stop, so there are no restarts to find. Treating those two the same
    invented phrase boundaries from punctuation against measured evidence.
    """
    if not words:
        return []

    measured = silences is not None
    pauses = [(s, e) for s, e in (silences or []) if e - s >= MIN_PAUSE]
    out, start_idx = [], 0

    def _flush(i0, i1):
        if i1 <= i0:
            return
        chunk = words[i0:i1]
        tokens = tuple(t for t in (_norm(w) for w in chunk) if t)
        if not tokens:
            return
        out.append(Phrase(
            start=chunk[0]["start"], end=chunk[-1]["end"], i0=i0, i1=i1,
            text=" ".join(_text(w) for w in chunk).strip(), tokens=tokens,
        ))

    for i, w in enumerate(words):
        boundary = False
        if measured:
            # A pause counts as a boundary when it sits at or after this word's
            # end and before the next word starts.
            nxt = words[i + 1]["start"] if i + 1 < len(words) else w["end"]
            boundary = any(p_start < nxt and p_end > w["end"] - 0.05
                           for p_start, p_end in pauses)
        elif _text(w).strip()[-1:] in CLAUSE_PUNCT:
            boundary = True
        if boundary:
            _flush(start_idx, i + 1)
            start_idx = i + 1

    _flush(start_idx, len(words))
    return out


def _shingles(tokens: tuple, n: int = 2) -> set:
    return {tokens[i:i + n] for i in range(len(tokens) - n + 1)}


def similarity(a: Phrase, b: Phrase) -> float:
    """
    How much of the shorter attempt shows up in the longer one.

    Overlap (intersection over the *smaller* set) rather than Jaccard, because
    a second attempt is routinely longer or shorter than the first and Jaccard
    punishes that. Bigrams carry word order, so a shared bag of common Hinglish
    connectives cannot score on its own.

    Returns 0 when the two are too lopsided in length to be takes of one line,
    or when the overlap is too small to mean anything regardless of ratio.
    """
    ta, tb = set(a.tokens), set(b.tokens)
    if not ta or not tb:
        return 0.0

    longer = max(a.duration, b.duration)
    if longer > 0 and min(a.duration, b.duration) / longer < MIN_DURATION_RATIO:
        return 0.0

    shared = ta & tb
    if len(shared) < MIN_SHARED_WORDS:
        return 0.0

    sa, sb = _shingles(a.tokens), _shingles(b.tokens)
    shared_bi = sa & sb
    if len(shared_bi) < MIN_SHARED_BIGRAMS:
        return 0.0

    uni = len(shared) / min(len(ta), len(tb))
    bi = len(shared_bi) / min(len(sa), len(sb)) if sa and sb else 0.0
    return 0.6 * uni + 0.4 * bi


def find_candidates(phrases: list, window: float = WINDOW,
                    min_similarity: float = MIN_SIMILARITY,
                    min_words: int = MIN_WORDS) -> list:
    """
    Group phrases that look like attempts at the same line.

    Returns [[Phrase, ...], ...] in time order, each group holding two or more
    attempts. Grouping (rather than pairing) means a line delivered four times
    goes up as one question instead of six.
    """
    usable = [p for p in phrases
              if min_words <= len(p.tokens) <= MAX_WORDS]
    group_of: dict = {}
    groups: list = []

    for j, later in enumerate(usable):
        best, best_score = None, min_similarity
        for earlier in reversed(usable[max(0, j - LOOKBACK):j]):
            if later.start - earlier.end > window:
                break
            score = similarity(earlier, later)
            if score >= best_score:
                best, best_score = earlier, score
        if best is None:
            continue
        gi = group_of.get(id(best))
        if gi is None:
            gi = len(groups)
            groups.append([best])
            group_of[id(best)] = gi
        groups[gi].append(later)
        group_of[id(later)] = gi

    return [g for g in groups if len(g) > 1]


# ── Stage 2: the model decides ───────────────────────────────────────────────

SYSTEM = (
    "You are helping edit a video. You will see attempts at a line of speech, "
    "in order, from one recording. Decide whether they are genuinely the "
    "speaker re-recording the same line after a flub, or separate things that "
    "merely sound alike. Speech in Hindi-English (Hinglish) reuses framing "
    "phrases constantly, so lexical overlap alone means nothing. Deliberate "
    "repetition for emphasis, and parallel structure in a list, are NOT "
    "retakes. Answer only with JSON."
)

PROMPT = """Attempts:
{attempts}

Reply with exactly this JSON:
{{"retake": true|false, "keep": <index of the attempt to keep>, "confidence": 0.0-1.0, "reason": "<one short clause>"}}

"retake" is false unless these are the same line re-recorded. When true, keep
the most complete, fluent delivery — the one without a stumble, a cut-off, or a
trailing self-correction. That is usually but not always the last attempt."""


def _ask(group: list, complete) -> dict | None:
    from . import llm
    attempts = "\n".join(
        f"[{i}] {p.start:.1f}s-{p.end:.1f}s: {p.text}" for i, p in enumerate(group)
    )
    raw = complete(PROMPT.format(attempts=attempts), system=SYSTEM, max_tokens=300)
    data = llm.parse_json(raw)
    if not isinstance(data, dict):
        return None
    # Exact types, not truthiness. JSON "false" is a truthy string, and Python
    # counts True as an int — so a sloppy check turns {"retake": "false"} and
    # {"keep": true} into real cuts, which is the one thing this must not do.
    if data.get("retake") is not True:
        return None
    keep = data.get("keep")
    if type(keep) is not int or not 0 <= keep < len(group):
        return None
    return data


def detect(words: list, media_path: str | None = None, silences: list | None = None,
           complete=None, max_groups: int = DEFAULT_MAX_GROUPS) -> dict:
    """
    Retake suggestions for a transcript.

    Returns {"cuts": [Cut, ...], "groups": [...], "status": str}. Cuts are
    always `auto=False`: a retake is seconds of real speech and gets confirmed
    by a person, every time.

    `complete` is injectable so the detector can be exercised without a network
    call. `status` explains an empty result: "no retakes", "no API key", and
    "the key was rejected" look identical from the outside and mean very
    different things.

    `max_groups` bounds cost on a long recording. Whatever it drops is reported
    in `skipped` and surfaced by both callers — a cap the user cannot see reads
    as "we checked everything".
    """
    from . import llm, tighten

    if complete is None:
        if not llm.available():
            return {"cuts": [], "groups": [], "status": "no-model",
                    "error": None, "asked": 0, "skipped": 0}
        complete = llm.complete

    if silences is None and media_path:
        try:
            silences = [(c.start, c.end) for c in
                        tighten.detect_silences_from_audio(media_path,
                                                           tighten.TightenConfig())]
        except Exception:                                   # noqa: BLE001
            silences = None

    phrases = split_phrases(words, silences)
    groups = find_candidates(phrases)
    if not groups:
        return {"cuts": [], "groups": [], "status": "none-found",
                "error": None, "asked": 0, "skipped": 0}

    cuts, reported, asked = [], [], 0
    for group in groups[:max_groups]:
        asked += 1
        try:
            verdict = _ask(group, complete)
        except llm.LLMError as e:
            # One failure means the rest will fail the same way — a rejected
            # key does not start working on the next call. Stop and say so,
            # rather than burning quota to report "nothing found".
            return {"cuts": cuts, "groups": reported, "status": "model-error",
                    "error": str(e), "asked": asked,
                    "skipped": max(0, len(groups) - asked)}
        if not verdict:
            continue
        keep = verdict["keep"]
        try:
            confidence = min(1.0, max(0.0, float(verdict.get("confidence") or 0.0)))
        except (TypeError, ValueError):
            confidence = 0.0
        dropped = []
        for i, p in enumerate(group):
            if i == keep:
                continue
            cuts.append(Cut(p.start, p.end, "retake", confidence,
                            text=p.text[:120], auto=False))
            dropped.append(i)
        reported.append({
            "keep": keep, "dropped": dropped,
            "confidence": confidence,
            "reason": str(verdict.get("reason") or "")[:200],
            "attempts": [{"start": p.start, "end": p.end, "text": p.text}
                         for p in group],
        })

    skipped = max(0, len(groups) - max_groups)
    status = "ok" if reported else "none-confirmed"
    return {"cuts": cuts, "groups": reported, "status": status, "error": None,
            "asked": asked, "skipped": skipped}
