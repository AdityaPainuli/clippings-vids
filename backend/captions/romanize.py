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

DEVANAGARI_RE = re.compile(r"[ऀ-ॿ]")
CHUNK = 80  # words per LLM call — small enough to keep alignment reliable

# IAST → colloquial fixes applied after transliteration (fallback path)
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
        c for c in unicodedata.normalize("NFD", latin) if not unicodedata.combining(c)
    )
    # IAST 'c' is the "ch" sound; 'ch' is aspirated "chh"
    latin = latin.replace("ch", "chh").replace("c", "ch").replace("chhh", "chh")
    # Final schwa deletion: "taraha" → "tarah" (skip short particles like "ka", "na")
    stripped = latin.rstrip(".,!?")
    if len(stripped) > 3 and stripped.endswith("a") and stripped[-2] not in "aeiou":
        latin = stripped[:-1] + latin[len(stripped):]
    return _COMMON.get(latin, latin)


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
        if isinstance(out, list) and len(out) == len(words):
            return [str(w) for w in out]
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
