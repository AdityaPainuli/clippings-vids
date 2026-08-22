"""
One small cloud-model call, over plain HTTP.

Bolcap ships without cloud SDKs on purpose (DECISIONS.md: the desktop binary
stays small, no torch, no vendor clients), so this talks to the APIs directly
with `requests`, which is already a dependency. Adding `anthropic` or
`google-generativeai` would put tens of megabytes into every platform build to
save a dozen lines here.

Nothing here runs unless the user sets a key. No key means the caller gets
None and the feature that wanted it simply stays off.

Only transcript *text* is ever sent. Audio and video never leave the machine.
"""

import json
import os

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = os.getenv("BOLCAP_ANTHROPIC_MODEL", "claude-sonnet-5")
GEMINI_MODEL = os.getenv("BOLCAP_GEMINI_MODEL", "gemini-2.5-flash")
TIMEOUT = 60


def provider() -> str | None:
    """Which cloud model is configured, if any."""
    if os.getenv("ANTHROPIC_API_KEY"):
        return "anthropic"
    if os.getenv("GOOGLE_API_KEY"):
        return "gemini"
    return None


def available() -> bool:
    return provider() is not None


def complete(prompt: str, system: str = "", max_tokens: int = 1024) -> str | None:
    """
    Prompt in, text out. Returns None on a missing key or any failure —
    callers treat this as "no answer", never as an error worth stopping for.
    """
    which = provider()
    if which is None:
        return None
    try:
        import requests
        if which == "anthropic":
            return _anthropic(requests, prompt, system, max_tokens)
        return _gemini(requests, prompt, system, max_tokens)
    except Exception as e:                                  # noqa: BLE001
        print(f"  [llm] call failed ({type(e).__name__}: {e})")
        return None


def _anthropic(requests, prompt: str, system: str, max_tokens: int) -> str | None:
    body = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system:
        body["system"] = system
    r = requests.post(
        ANTHROPIC_URL, timeout=TIMEOUT,
        headers={"x-api-key": os.environ["ANTHROPIC_API_KEY"],
                 "anthropic-version": "2023-06-01",
                 "content-type": "application/json"},
        data=json.dumps(body),
    )
    if r.status_code != 200:
        print(f"  [llm] anthropic {r.status_code}: {r.text[:200]}")
        return None
    parts = r.json().get("content", [])
    return "".join(p.get("text", "") for p in parts if p.get("type") == "text") or None


def _gemini(requests, prompt: str, system: str, max_tokens: int) -> str | None:
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent")
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"maxOutputTokens": max_tokens},
    }
    if system:
        body["systemInstruction"] = {"parts": [{"text": system}]}
    r = requests.post(
        url, timeout=TIMEOUT,
        headers={"x-goog-api-key": os.environ["GOOGLE_API_KEY"],
                 "content-type": "application/json"},
        data=json.dumps(body),
    )
    if r.status_code != 200:
        print(f"  [llm] gemini {r.status_code}: {r.text[:200]}")
        return None
    try:
        parts = r.json()["candidates"][0]["content"]["parts"]
    except (KeyError, IndexError):
        return None
    return "".join(p.get("text", "") for p in parts) or None


def parse_json(text: str | None):
    """
    Pull a JSON value out of a model reply, fenced or not.

    Models wrap JSON in ```json fences often enough that failing on it would
    make the feature flaky for no reason.
    """
    if not text:
        return None
    body = text.strip()
    if "```" in body:
        chunk = body.split("```")[1]
        if chunk[:4].lower() == "json":
            chunk = chunk[4:]
        body = chunk.strip()
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        start = min((i for i in (body.find("{"), body.find("[")) if i >= 0),
                    default=-1)
        end = max(body.rfind("}"), body.rfind("]"))
        if start >= 0 and end > start:
            try:
                return json.loads(body[start:end + 1])
            except json.JSONDecodeError:
                return None
        return None
