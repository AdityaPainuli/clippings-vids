"""
Small provider-backed model calls over plain HTTP.

Bolcap ships without cloud SDKs on purpose. This talks to the APIs directly
with `requests`, which is already a dependency.

Cloud providers use their configured API keys. The OpenAI-compatible provider
is intended for local endpoints such as Ollama, so it does not require an API
key.

A call that fails raises LLMError rather than returning None. A bad key, a
quota wall, and a model that simply says "no" are three different outcomes.

Only transcript text is ever sent. Audio and video never leave the machine.
"""

import json
import os

# Canonical provider → environment variable map. The desktop app's config
# layer reads this rather than keeping its own copy, so the two can never
# disagree about where a key lives.
PROVIDER_ENV = {
    "anthropic": "ANTHROPIC_API_KEY",
    "gemini": "GOOGLE_API_KEY",
    "openai-compatible": None,
}

# Set by the app when a key came from the launch environment. That choice is
# deliberate and has to win: injecting a saved Anthropic key would otherwise
# silently outrank a GOOGLE_API_KEY the user exported on purpose.
PREFERRED_ENV = "BOLCAP_LLM_PROVIDER"

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = os.getenv("BOLCAP_ANTHROPIC_MODEL", "claude-sonnet-5")
GEMINI_MODEL = os.getenv("BOLCAP_GEMINI_MODEL", "gemini-2.5-flash")
OPENAI_COMPATIBLE_BASE_URL = os.getenv(
    "BOLCAP_LLM_BASE_URL",
    "http://localhost:11434/v1",
)
OPENAI_COMPATIBLE_MODEL = os.getenv(
    "BOLCAP_LLM_MODEL",
    "llama3.2",
)
TIMEOUT = 60


class LLMError(RuntimeError):
    """The call did not complete. Distinct from the model answering 'no'."""


def _local_available(requests) -> bool:
    try:
        r = requests.get(
            f"{OPENAI_COMPATIBLE_BASE_URL.rstrip('/')}/models",
            timeout=5,
        )
        return r.status_code == 200
    except requests.RequestException:
        return False


def provider() -> str | None:
    """Which model provider is configured and available."""
    preferred = os.getenv(PREFERRED_ENV)

    if preferred == "openai-compatible":
        try:
            import requests
        except ImportError:
            return None
        return preferred if _local_available(requests) else None

    if preferred in PROVIDER_ENV:
        env = PROVIDER_ENV[preferred]
        if env and os.getenv(env):
            return preferred
        if preferred in ("anthropic", "gemini"):
            return None

    for name, env in PROVIDER_ENV.items():
        if env and os.getenv(env):
            return name

    try:
        import requests
    except ImportError:
        return None

    if _local_available(requests):
        return "openai-compatible"

    return None


def available() -> bool:
    return provider() is not None

def complete(prompt: str, system: str = "", max_tokens: int = 1024) -> str | None:
    """
    Prompt in, text out. Returns None when no provider is configured or available.

    Raises LLMError when the call itself fails, so the caller can tell an
    outage from an answer.
    """
    which = provider()

    if which is None:
        return None

    try:
        import requests
    except ImportError as e:
        raise LLMError(f"requests is not installed ({e})") from e

    try:
        if which == "anthropic":
            return _anthropic(requests, prompt, system, max_tokens)

        if which == "gemini":
            return _gemini(requests, prompt, system, max_tokens)

        if which == "openai-compatible":
            return _openai_compatible(requests, prompt, system, max_tokens)

        raise LLMError(f"Unknown LLM provider: {which}")

    except LLMError:
        raise
    except Exception as e:  # noqa: BLE001
        raise LLMError(f"{type(e).__name__}: {e}") from e


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
        raise LLMError(_http_reason("Anthropic", r))
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
        raise LLMError(_http_reason("Gemini", r))
    try:
        parts = r.json()["candidates"][0]["content"]["parts"]
    except (KeyError, IndexError):
        return None
    return "".join(p.get("text", "") for p in parts) or None


def _openai_compatible(
    requests,
    prompt: str,
    system: str,
    max_tokens: int,
) -> str | None:
    messages = []

    if system:
        messages.append({"role": "system", "content": system})

    messages.append({"role": "user", "content": prompt})

    body = {
        "model": OPENAI_COMPATIBLE_MODEL,
        "messages": messages,
        "max_tokens": max_tokens,
    }

    url = f"{OPENAI_COMPATIBLE_BASE_URL.rstrip('/')}/chat/completions"

    r = requests.post(
        url,
        timeout=TIMEOUT,
        headers={"content-type": "application/json"},
        data=json.dumps(body),
    )

    if r.status_code != 200:
        raise LLMError(_http_reason("OpenAI-compatible", r))

    try:
        return r.json()["choices"][0]["message"]["content"] or None
    except (KeyError, IndexError, TypeError):
        return None


def _http_reason(name: str, r) -> str:
    """A message the user can act on, not a status code to look up."""
    if r.status_code in (401, 403):
        return f"{name} rejected the API key (HTTP {r.status_code})"
    if r.status_code == 429:
        return f"{name} rate limit or quota reached (HTTP 429)"
    if r.status_code >= 500:
        return f"{name} is having trouble (HTTP {r.status_code})"
    detail = (r.text or "").strip().replace("\n", " ")[:160]
    return f"{name} returned HTTP {r.status_code}: {detail}"


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
