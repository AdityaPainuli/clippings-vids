"""
Code-mixed language detection at segment/word level.

Tags every word in a transcript with a ``lang`` code — one of:
  "hi"  Hindi (Devanagari or Hinglish Latin)
  "ta"  Tamil (Tanglish Latin)
  "te"  Telugu (Tenglish Latin)
  "en"  English

Design constraints:
  * Zero new model dependencies.  Detection is purely lexicon + script-based.
  * Mid-utterance switches are handled at word granularity.
  * Latin-script disambiguation: when a word is in the Latin alphabet we use
    per-language vocabulary sets to decide whether it is regional or English.
    English technical nouns and proper nouns inside a regional sentence are
    tagged "en"; regional filler/function words are tagged with their language.
  * Devanagari script → always "hi".
  * If no evidence, the caller's segment-level language hint (from Whisper's
    auto-detect) is inherited, with "en" as the final fallback.

Backward-compatibility guarantee:
  * If the caller passes words that already have a ``lang`` key, those are
    left unchanged and returned as-is.
  * If no language evidence at all is present, words gain ``lang`` equal to
    the ``segment_lang`` hint or "en" — identical downstream behaviour to the
    pre-patch baseline.
"""

import re
import unicodedata

# ── Script ranges ─────────────────────────────────────────────────────────────

DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]")
TAMIL_SCRIPT_RE = re.compile(r"[\u0B80-\u0BFF]")
TELUGU_SCRIPT_RE = re.compile(r"[\u0C00-\u0C7F]")
LATIN_RE = re.compile(r"^[A-Za-z'\-]+$")


# ── Lexicon sets (Latin-script only; each is a set of lowercased tokens) ──────

# Core Hindi function/filler words that appear in Hinglish transliteration.
# These are words that a Latin-script Hindi speaker would write but an English
# speaker would not.
_HINDI_LATIN = {
    # Pronouns & particles
    "main", "mein", "hum", "hamare", "aap", "tum", "woh", "yeh", "ye", "wo",
    "kya", "hai", "hain", "tha", "thi", "the", "hoga", "hogi", "honge",
    "ka", "ki", "ke", "ko", "se", "par", "tak", "bhi", "hi",
    "na", "nahi", "nahin", "nhin",
    # Conjunctions/connectives
    "aur", "lekin", "magar", "kyunki", "isliye", "toh", "to",
    "agar", "phir", "jab", "tab", "jabtak",
    # Common verbs (infinitive/present)
    "karna", "karo", "kar", "kiya", "kiye", "karte", "karti", "karein",
    "karta", "karti",
    "dena", "do", "diya", "lena", "lo", "liya", "aana", "jaan", "sochna",
    "dekhna", "dekho", "dekha", "baat", "samajhna",
    "bhejta", "leke",
    # Fillers & discourse markers (lexical)
    "matlab", "yaani", "yani", "waise", "aisa", "aise", "bilkul",
    "achha", "acha", "haan", "arre", "yaar", "bas", "sahi", "thik",
    "theek",
    # Nouns & adjectives common in Hinglish
    "paise", "kaam", "log", "cheez", "jagah", "waqt", "din",
    "roz", "baar", "thoda", "bahut", "zyada", "kam", "acchi", "bura",
    "naya", "purana", "chhota", "bada", "lamba",
    # Common demonstratives, quantifiers, adverbs
    "teen", "pehla", "doosra", "teesra", "itna", "dekhiye", "hamare",
    "wala", "yeh", "wo",
    # Reduplication triggers (these never appear as English)
    "alag", "dheere", "jaldi", "kabhi", "saath", "door", "paas", "kuch",
}

# Core Tamil function/filler words written in Latin (Tanglish).
_TAMIL_LATIN = {
    # Pronouns
    "naan", "naanga", "nee", "neenga", "avan", "aval", "avanga",
    "oru", "itha", "idha", "anga", "inga", "engey",
    # Verbs (common)
    "pogo", "poren", "varen", "solren", "pakuren", "panren",
    "irukku", "irukken", "illa", "illai", "irukkum",
    "panni", "kandupidikkirom", "solren", "pannuvanga", "pannirukkeengala",
    # Particles & common function words
    "antha", "ana", "aana", "aprom", "appuram", "ellam", "konjam",
    "romba", "mokka", "sari", "enna", "yenna", "endha",
    "aa", "la", "da", "di", "dei", "machan", "mama", "pa", "bro",
    "pola", "maari", "mathiri", "theriyum", "therila", "theriyuma",
    "vandhu", "pottu", "vechu", "eduthu",
    "pakkalaam", "solla", "paaru",
}

# Core Telugu function/filler words written in Latin (Tenglish).
_TELUGU_LATIN = {
    # Pronouns
    "nenu", "meeru", "memu", "vaadu", "aame", "vaalluu", "mana",
    "idi", "adi", "ika", "ikkade", "akkade",
    # Verbs (common)
    "chestunna", "cheyyadam", "cheppanu", "cheppu", "poni",
    "veltunna", "vastunna", "undi", "ledu",
    "chestundi", "chestaam",
    # Particles & conjunctions
    "ante", "ayithe", "kaani", "kani", "mari", "aina", "oka", "okka",
    "anni", "chala", "chaaala", "konni", "koncham",
    "ga", "emo", "ra", "babai", "anna", "akka", "bro",
    "ayya", "amma",
    "manchidi", "baagundi", "bagundi", "thelusaa", "telusaa", "telusa",
    "cheyyi", "cheyyadam", "enti",
}

# Words that look like they could be regional but are standard English.
# These override the regional lexicons when the segment_lang hint is "en"
# or when the word is unambiguously English-origin.
_ENGLISH_OVERRIDE = {
    # Common English words that happen to match a regional pattern
    "so", "do", "ok", "okay", "right", "like", "well", "actually",
    "basically", "literally", "obviously", "really", "just", "also",
    "but", "and", "or", "the", "a", "an", "in", "on", "at", "by",
    "for", "to", "of", "with", "from", "that", "this", "it", "is",
    "are", "was", "were", "be", "been", "being", "have", "has", "had",
    "will", "would", "can", "could", "should", "may", "might", "must",
    "not", "no", "yes", "hello", "hi", "hey", "thanks", "thank",
    "please", "sorry", "okay", "great", "good", "bad", "new", "old",
    "first", "second", "one", "two", "three", "time", "day", "year",
    "people", "way", "work", "know", "think", "see", "come", "go",
    "get", "make", "use", "want", "need", "say", "tell", "give", "take",
}


_NONLEXICAL_SOUNDS = {
    "um", "umm", "ummm", "uh", "uhh", "uhm", "erm", "er", "hmm", "hmmm",
    "mmm", "mm", "ahh", "uhhh", "eh", "err", "ah",
}


# ── Core detection ────────────────────────────────────────────────────────────

def _script_lang(text: str) -> str | None:
    """Return a lang code based purely on Unicode script, or None if Latin/mixed."""
    stripped = re.sub(r"[^\w]", "", text, flags=re.UNICODE)
    if not stripped:
        return None
    if DEVANAGARI_RE.search(stripped):
        return "hi"
    if TAMIL_SCRIPT_RE.search(stripped):
        return "ta"
    if TELUGU_SCRIPT_RE.search(stripped):
        return "te"
    return None  # Latin or digit — needs lexicon disambiguation


def _latin_lang(token: str, segment_lang: str) -> str:
    """
    Disambiguate a Latin-script token.

    Priority order:
    1. Unambiguous English word → "en"
    2. Non-lexical hesitation sounds in regional speech → inherit segment_lang
    3. Regional lexicon match → regional code
    4. Unmatched Latin word (technical vocabulary, loan words) → "en"
    """
    t = token.lower().strip(".,!?;:\"'-–—")
    if not t:
        return segment_lang or "en"

    # Pure digit tokens are not language-bearing
    if t.isdigit():
        return segment_lang or "en"

    # If the segment hint is already English and the word is in the English
    # override set, trust that — don't pull it into a regional language.
    if segment_lang == "en" and t in _ENGLISH_OVERRIDE:
        return "en"

    # Non-lexical hesitations in regional speech inherit the segment language
    if t in _NONLEXICAL_SOUNDS and segment_lang in ("hi", "ta", "te"):
        return segment_lang

    # Regional lexicons take priority for known regional vocabulary.
    if t in _HINDI_LATIN:
        return "hi"
    if t in _TAMIL_LATIN:
        return "ta"
    if t in _TELUGU_LATIN:
        return "te"

    # If the segment language is regional and the word isn't in any English
    # override list, treat it as regional (loan words, proper nouns, etc.).
    # BUT: we only apply this if the word looks like a known regional pattern.
    # An unknown Latin word (e.g. "backend", "project", "API") that doesn't
    # match any regional lexicon entry defaults to "en" — Latin script is the
    # writing system of English, and open-ended English technical vocabulary
    # cannot be exhaustively listed.  Regional words are a closed functional
    # vocabulary that we do enumerate.
    #
    # Summary: regional lexicon match → regional; no match → "en".
    return "en"


def tag_words(
    words: list[dict],
    segment_lang: str | None = None,
) -> list[dict]:
    """
    Add a ``lang`` key to every word dict.

    Parameters
    ----------
    words:
        List of word dicts, each with at least ``{"start", "end", "text"}``.
        Words that already have a ``lang`` key are returned unchanged.
    segment_lang:
        The ISO-639-1 language code reported by Whisper for this segment
        (e.g. ``"hi"``, ``"en"``, ``"ta"``, ``"te"``).  Used as the
        fallback when per-word script/lexicon evidence is inconclusive.
        Pass ``None`` to mean "unknown"; the ultimate default is ``"en"``.

    Returns
    -------
    The same list, each word augmented with ``"lang"``.
    """
    base = segment_lang or "en"
    tagged = []
    for w in words:
        if "lang" in w:
            tagged.append(w)
            continue
        text = (w.get("hinglish") or w.get("text") or "").strip()
        script_hit = _script_lang(text)
        norm_text = text.strip(".,!?;:\"'-–— \t\n")
        if script_hit is not None:
            lang = script_hit
        elif LATIN_RE.match(norm_text.replace(" ", "")):
            lang = _latin_lang(text, base)
        else:
            lang = base
        tagged.append({**w, "lang": lang})
    return tagged
