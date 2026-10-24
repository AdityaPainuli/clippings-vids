"""
Hinglish romanization — Devanagari words → natural Latin spellings.

Primary path: Gemini rewrites the word list the way people actually type
Hinglish ("chunautiyan", not IAST "cunatiyam"), preserving 1:1 word
alignment so timings survive. Fallback path: rule-based transliteration
(indic-transliteration + cleanup) when no API key is configured.

Lang-tag aware: if a word carries a ``lang`` tag from lang_detect, only
words tagged ``"hi"`` with Devanagari script are sent through the LLM/rule
pipeline. Latin-script words of any lang pass through unchanged, as does
any word already carrying a ``hinglish`` field.
"""

import json
import os
import re
import unicodedata
from difflib import SequenceMatcher

DEVANAGARI_RE = re.compile(r"[ऀ-ॿ]")
CHUNK = 80  # words per LLM call — small enough to keep alignment reliable
MIN_LLM_SIMILARITY = 0.45
MIN_LLM_LENGTH_RATIO = 0.6
MAX_LLM_LENGTH_RATIO = 1.8

# IAST → colloquial fixes applied after transliteration (fallback path)
_COMMON = {
    "maim": "main", "mem": "mein", "aura": "aur", "hama": "hum",
    "hai": "hai", "haim": "hain", "nahim": "nahin", "kya": "kya",
}

_VIRAMA = "\u094d"
_NUKTA = "\u093c"
_SHORT_I = "\u093f"
# ZWJ and ZWNJ only choose how a conjunct is drawn, so वक्त is the same word
# with or without one
_JOINERS = "\u200c\u200d"
# Letters, vowel signs and nasals; stops at danda, digits and punctuation.
# Join controls stay inside the run so they don't cut a word in two
_DEVANAGARI_RUN = re.compile(
    r"[\u0900-\u0963\u0971-\u097f]+(?:[\u200c\u200d]+[\u0900-\u0963\u0971-\u097f]+)*")
# ड़ and ढ़ are typed "d" and "dh" in Hinglish (bada, padhna), but IAST
# writes them r̤ and r̤h, which flatten to "bar" and "parh". Whisper can
# emit either the precomposed letter or letter + nukta.
_FLAPS = {"\u095c": "\u0921", "\u0921\u093c": "\u0921",
          "\u095d": "\u0922", "\u0922\u093c": "\u0922"}
# IAST 'c' is च and 'ch' is छ. Hinglish types both as "ch" (chalo, chota)
# and the doubled च्छ as "chh" (achha).
_CH = {"cch": "chh", "ch": "ch", "c": "ch"}
# Schwa deletion stops at a prefix boundary, which the syllable rule can't
# see: अ + समान is "asaman", not "asman". (prefix, stem)
_PREFIXED = (("अ", "समान"), ("अ", "समर्थ"), ("अ", "समय"), ("अ", "सहाय"),
             ("अ", "सहज"), ("अ", "सफल"), ("ना", "समझ"))

# Verb endings whose stem-final schwa always drops. The syllable scan cannot
# see this boundary: मां-ग-ना and ज़िं-द-गी have the same shape, but the first
# is a verb stem plus suffix and the second keeps its medial schwa.
_VERB_SUFFIXES = ("ना", "ता", "ती", "ते")


def _is_consonant(ch: str) -> bool:
    return "\u0915" <= ch <= "\u0939" or "\u0958" <= ch <= "\u095f"


def _drop_schwas(run: str) -> str:
    """
    Add a virama wherever Hindi doesn't pronounce the inherent vowel.

    This has to happen on the Devanagari. After transliteration IAST ā is
    flattened to a, and then the inherent schwa of "kamara" and the written
    ा of "choṭā" look the same, so a rule on the Latin side ends up deleting
    real vowels.

    Final schwa goes ("taraha" → "tarah") unless it is the word's only vowel
    ("na"). Medial schwa goes between a vowel-consonant and a consonant-vowel
    ("kamarā" → "kamra"), scanning right to left so one deletion blocks the
    one before it and no three-consonant cluster appears ("samajhna").
    Anusvara and visarga close a syllable like a consonant would, so the
    schwa after them stays ("zindagi", not "zindgi"). A word-final short ि
    marks a Sanskrit word, which keeps the schwa before it ("pragati",
    "senapati").
    """
    for prefix, stem in _PREFIXED:
        if run.startswith(prefix + stem):
            return _drop_schwas(prefix) + _drop_schwas(run[len(prefix):])
    for suffix in _VERB_SUFFIXES:
        if run.endswith(suffix) and len(run) > len(suffix):
            stem = run[:-len(suffix)]
            return _drop_schwas(stem) + _drop_schwas(suffix)
    # [text, vowel]: vowel is "a" for the inherent schwa, "v" for an open
    # written vowel, "n" for a syllable closed by anusvara/visarga, "" after
    # a virama
    syl = []
    for ch in run:
        if ch in _JOINERS:
            continue   # no sound and no meaning in Latin script
        if _is_consonant(ch):
            syl.append([ch, "a"])
        elif not syl or "\u0904" <= ch <= "\u0914":
            syl.append([ch, "v"])
        else:
            syl[-1][0] += ch
            if ch == _VIRAMA:
                syl[-1][1] = ""
            elif ch in "\u0902\u0903":
                syl[-1][1] = "n"
            elif ch != _NUKTA and syl[-1][1] == "a":
                syl[-1][1] = "v"   # matra or chandrabindu

    def drop(i):
        syl[i][0] += _VIRAMA
        syl[i][1] = ""

    if syl and syl[-1][1] == "a" and any(s[1] for s in syl[:-1]):
        drop(-1)
    for i in range(len(syl) - 2, 0, -1):
        if (syl[i][1] == "a" and syl[i - 1][1] in ("a", "v") and syl[i + 1][1]
                and _is_consonant(syl[i + 1][0][0])
                and not (i == len(syl) - 2 and syl[-1][0].endswith(_SHORT_I))):
            drop(i)
    return "".join(s[0] for s in syl)


def _rule_romanize(text: str) -> str:
    from indic_transliteration import sanscript
    from indic_transliteration.sanscript import transliterate

    for flap, plain in _FLAPS.items():
        text = text.replace(flap, plain)
    text = _DEVANAGARI_RUN.sub(lambda m: _drop_schwas(m.group()), text)
    latin = transliterate(text, sanscript.DEVANAGARI, sanscript.IAST)
    latin = latin.replace("ṃ", "n").replace("m̐", "n")
    latin = "".join(
        c for c in unicodedata.normalize("NFD", latin) if not unicodedata.combining(c)
    )
    latin = re.sub(r"cch|ch|c", lambda m: _CH[m.group()], latin)
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

        # Latin-script input must survive the LLM untouched. The model is a
        # romanizer, not a translator or text rewriter.
        if not DEVANAGARI_RE.search(source):
            if candidate != source:
                return False
            continue

        # Devanagari input must become a single Latin-script token. This also
        # rejects translated/rephrased output that happens to keep alignment.
        if DEVANAGARI_RE.search(candidate) or re.search(r"[^A-Za-z0-9'.,!?;:()\"%&/\-+ ]", candidate):
            return False
        if any(ch.isspace() for ch in candidate.strip()):
            return False

        # Compare against the deterministic transliteration as a semantic
        # guardrail. Natural Hinglish spellings vary, so this is intentionally
        # a permissive similarity floor rather than an exact match.
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
    """One Gemini call for a chunk. Returns None on any failure or misalignment."""
    try:
        import google.generativeai as genai
    except ImportError:
        return None
    if not os.getenv("GOOGLE_API_KEY"):
        return None

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
        model = genai.GenerativeModel(model_name="gemini-2.5-flash")
        response = model.generate_content(prompt)
        text = response.text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.lower().startswith("json"):
                text = text[4:]
        out = json.loads(text.strip())
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
    3. No tag: fall back to DEVANAGARI_RE scan (legacy/untagged transcripts).
    """
    if word.get("hinglish"):
        return False
    text = word.get("text", "")
    lang = word.get("lang")
    if lang is not None:
        return lang == "hi" and bool(DEVANAGARI_RE.search(text))
    # Legacy path — no lang tag present
    return bool(DEVANAGARI_RE.search(text))


def romanize_words(words: list[dict]) -> list[dict]:
    """
    Input/output: [{"start", "end", "text", ...}, ...]. Adds ``hinglish`` to
    each word that needs Devanagari → Latin conversion; ``text`` keeps the
    original script so edits stay lossless.

    Words carrying an explicit ``lang`` tag other than ``"hi"`` are always
    passed through untouched (they are already in Latin script).
    """
    texts = [w["text"] for w in words]
    romanized: list[str] = []

    for i in range(0, len(texts), CHUNK):
        chunk_words = words[i : i + CHUNK]
        chunk_texts = texts[i : i + CHUNK]
        # Only invoke the LLM/rule path when this chunk actually contains
        # words that need romanization.
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
