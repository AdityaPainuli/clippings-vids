"""
Hinglish romanization — Devanagari words → natural Latin spellings.

Primary path: Gemini rewrites the word list the way people actually type
Hinglish ("chunautiyan", not IAST "cunatiyam"), preserving 1:1 word
alignment so timings survive. Fallback path: rule-based transliteration
(indic-transliteration + cleanup) when no API key is configured.
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


def romanize_words(words: list[dict]) -> list[dict]:
    """
    Input/output: [{"start", "end", "text"}, ...]. Adds "hinglish" to each
    word; "text" keeps the original script so edits stay lossless.
    """
    texts = [w["text"] for w in words]
    romanized: list[str] = []

    for i in range(0, len(texts), CHUNK):
        chunk = texts[i : i + CHUNK]
        if not any(DEVANAGARI_RE.search(t) for t in chunk):
            romanized.extend(chunk)
            continue
        out = _llm_romanize_chunk(chunk)
        if out is None:
            out = [_rule_romanize(t) if DEVANAGARI_RE.search(t) else t for t in chunk]
        romanized.extend(out)

    return [
        {**w, "hinglish": h}
        for w, h in zip(words, romanized)
    ]
