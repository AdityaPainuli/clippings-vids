"""
Hinglish romanization - Devanagari words → natural Latin spellings.

Primary path: the configured LLM provider rewrites the word list the way people
actually type Hinglish ("chunautiyan", not IAST "cunatiyam"), preserving 1:1
word alignment so timings survive. Fallback path: rule-based transliteration
(indic-transliteration + cleanup) when no LLM is configured or available.

Lang-tag aware: if a word carries a ``lang`` tag from lang_detect, only words
tagged ``"hi"`` with Devanagari script are sent through the LLM/rule pipeline.
Latin-script words of any lang pass through unchanged, as does any word already
carrying a ``hinglish`` field.
"""

import json
import re
import unicodedata
from difflib import SequenceMatcher

from . import llm

DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]")
CHUNK = 80  # words per LLM call — small enough to keep alignment reliable
MIN_LLM_SIMILARITY = 0.45
MIN_LLM_LENGTH_RATIO = 0.6
MAX_LLM_LENGTH_RATIO = 1.8

_COMMON = {
    "maim": "main", "mem": "mein", "aura": "aur", "hama": "hum",
    "hai": "hai", "haim": "hain", "nahim": "nahin", "kya": "kya",
}


def _rule_romanize(text: str) -> str:
    from indic_transliteration import sanscript
    from indic_transliteration.sanscript import transliterate

    latin = transliterate(text, sanscript.DEVANAGARI, sanscript.IAST)
    latin = latin.replace("ṃ", "n").replace("m̐", "n")
    latin = "".join(
        c for c in unicodedata.normalize("NFD", latin)
        if not unicodedata.combining(c)
    )

    latin = latin.replace("ch", "chh").replace("c", "ch").replace("chhh", "chh")

    stripped = latin.rstrip(".,!?")
    if len(stripped) > 3 and stripped.endswith("a") and stripped[-2] not in "aeiou":
        latin = stripped[:-1] + latin[len(stripped):]

    return _COMMON.get(latin, latin)


def _normalized_latin(text: str) -> str:
    """Reduce romanized text to letters/digits for spelling comparison."""
    return re.sub(r"[^a-z0-9]", "", text.lower())


def _valid_llm_output(words: list[str], output: object) -> bool:
    """Validate an LLM response before allowing it into caption timings."""
    if not isinstance(output, list) or len(output) != len(words):
        return False

    for source, candidate in zip(words, output):
        if not isinstance(candidate, str) or not candidate.strip():
            return False

        # Latin-script input must survive the LLM untouched.
        if not DEVANAGARI_RE.search(source):
            if candidate != source:
                return False
            continue

        # Devanagari input must become a single Latin-script token.
        if DEVANAGARI_RE.search(candidate) or re.search(
            r"[^A-Za-z0-9'.,!?;:()\"%&/\-+ ]", candidate
        ):
            return False

        if any(ch.isspace() for ch in candidate.strip()):
            return False

        # Compare against deterministic transliteration as a semantic guardrail.
        baseline = _normalized_latin(_rule_romanize(source))
        actual = _normalized_latin(candidate)

        if not baseline or not actual:
            return False

        length_ratio = len(actual) / len(baseline)

        if not MIN_LLM_LENGTH_RATIO <= length_ratio <= MAX_LLM_LENGTH_RATIO:
            return False

        if SequenceMatcher(None, baseline, actual).ratio() < MIN_LLM_SIMILARITY:
            return False

    return True


def _llm_romanize_chunk(words: list[str]) -> list[str] | None:
    """One LLM call for a chunk. Returns None on any failure or misalignment."""
    prompt = (
        "Convert each Hindi word to natural Hinglish (Latin script, the way people "
        "type in WhatsApp/Instagram captions — e.g. क्या→kya, चुनौतियां→chunautiyan, "
        "मैं→main). Words already in Latin script pass through unchanged. Keep any "
        "punctuation attached to the word. Fix obvious speech-to-text spelling errors "
        "to the intended word.\n"
        "Return ONLY a JSON array of strings, same length and order as the input.\n\n"
        f"Input ({len(words)} words):\n{json.dumps(words, ensure_ascii=False)}"
    )

    try:
        text = llm.complete(prompt)

        if text is None:
            return None

        out = llm.parse_json(text)

        if _valid_llm_output(words, out):
            return out

    except Exception as e:
        print(f"  [romanize] LLM chunk failed ({e}), falling back to rules")

    return None


def _needs_romanize(word: dict) -> bool:
    """
    True when a word requires Devanagari → Latin conversion.

    Decision order:
    1. Already has a ``hinglish`` field → already done, skip.
    2. Has an explicit ``lang`` tag:
       - ``"hi"`` + Devanagari script → needs romanization.
       - Any other lang, or ``"hi"`` already in Latin → pass through.
    3. No tag: fall back to DEVANAGARI_RE scan.
    """
    if word.get("hinglish"):
        return False

    text = word.get("text", "")
    lang = word.get("lang")

    if lang is not None:
        return lang == "hi" and bool(DEVANAGARI_RE.search(text))

    return bool(DEVANAGARI_RE.search(text))


def romanize_words(words: list[dict]) -> list[dict]:
    """
    Input/output: [{"start", "end", "text", ...}, ...].
    Adds ``hinglish`` to each word that needs Devanagari → Latin conversion.
    ``text`` keeps the original script so edits stay lossless.
    """
    texts = [w["text"] for w in words]
    romanized: list[str] = []

    for i in range(0, len(texts), CHUNK):
        chunk_words = words[i : i + CHUNK]
        chunk_texts = texts[i : i + CHUNK]

        if not any(_needs_romanize(w) for w in chunk_words):
            romanized.extend(chunk_texts)
            continue

        out = _llm_romanize_chunk(chunk_texts)

        if out is None:
            out = [
                _rule_romanize(t) if _needs_romanize(w) else t
                for w, t in zip(chunk_words, chunk_texts)
            ]

        romanized.extend(out)

    return [
        {**w, "hinglish": h}
        for w, h in zip(words, romanized)
    ]