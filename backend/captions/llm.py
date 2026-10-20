"""
One small cloud-model call, over plain HTTP.

Bolcap ships without cloud SDKs on purpose (DECISIONS.md: the desktop binary
stays small, no torch, no vendor clients), so this talks to the APIs directly
with `requests`, which is already a dependency. Adding `anthropic` or
`google-generativeai` would put tens of megabytes into every platform build to
save a dozen lines here.

Nothing here runs unless the user sets a key. No key means the caller gets
None and the feature that wanted it simply stays off.

A call that *fails* raises LLMError rather than returning None. A bad key, a
quota wall, and a model that simply says "no" are three different outcomes, and
collapsing them made an outage read to the user as "nothing found".

Only transcript *text* is ever sent. Audio and video never leave the machine.
"""

import json
import os

# Canonical provider → environment variable map. The desktop app's config
# layer reads this rather than keeping its own copy, so the two can never
# disagree about where a key lives.
PROVIDER_ENV = {"anthropic": "ANTHROPIC_API_KEY", "gemini": "GOOGLE_API_KEY"}

# Set by the app when a key came from the launch environment. That choice is
# deliberate and has to win: injecting a saved Anthropic key would otherwise
# silently outrank a GOOGLE_API_KEY the user exported on purpose.
PREFERRED_ENV = "BOLCAP_LLM_PROVIDER"

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = os.getenv("BOLCAP_ANTHROPIC_MODEL", "claude-sonnet-5")
GEMINI_MODEL = os.getenv("BOLCAP_GEMINI_MODEL", "gemini-2.5-flash")
TIMEOUT = 60
# backend/captions/llm.py (Excerpt)

import os
import requests
import logging

logger = logging.getLogger(__name__)

# Extend PROVIDER_ENV to include openai/local endpoint (no API key required)
PROVIDER_ENV = {
    "anthropic": "ANTHROPIC_API_KEY",
    "gemini": "GOOGLE_API_KEY",
    "openai": None  # Local / OpenAI-compatible endpoint requires no key
}

DEFAULT_OPENAI_BASE_URL = os.getenv("BOLCAP_OPENAI_BASE_URL", "http://localhost:11434/v1")
DEFAULT_OPENAI_MODEL = os.getenv("BOLCAP_OPENAI_MODEL", "llama3")

class LLMError(Exception):
    """Raised when LLM request fails or returns invalid structured output."""
    pass

def is_provider_available(provider_name: str) -> bool:
    if provider_name == "openai":
        base_url = os.getenv("BOLCAP_OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL)
        try:
            # Lightweight health or model check to confirm endpoint is reachable
            response = requests.get(f"{base_url.rstrip('/')}/models", timeout=2)
            return response.status_code == 200
        except Exception:
            return False
            
    env_var = PROVIDER_ENV.get(provider_name)
    return bool(env_var and os.getenv(env_var))

def chat_completion(prompt: str, system_prompt: str = None) -> str:
    provider = os.getenv("BOLCAP_LLM_PROVIDER") or _auto_detect_provider()
    
    if provider == "openai":
        return _call_openai_compatible(prompt, system_prompt)
    elif provider == "anthropic":
        return _call_anthropic(prompt, system_prompt)
    elif provider == "gemini":
        return _call_gemini(prompt, system_prompt)
    else:
        raise LLMError("No valid LLM provider available or configured.")

def _call_openai_compatible(prompt: str, system_prompt: str = None) -> str:
    base_url = os.getenv("BOLCAP_OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL)
    model = os.getenv("BOLCAP_OPENAI_MODEL", DEFAULT_OPENAI_MODEL)
    
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})
    
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.1
    }
    
    try:
        response = requests.post(
            f"{base_url.rstrip('/')}/chat/completions",
            json=payload,
            timeout=30
        )
        if response.status_code != 200:
            raise LLMError(f"OpenAI-compatible endpoint returned status {response.status_code}: {response.text}")
            
        data = response.json()
        return data["choices"][0]["message"]["content"].strip()
    except requests.exceptions.ConnectionError as conn_err:
        logger.warning(f"[llm] Connection refused to local endpoint {base_url}. Falling back to rules.")
        raise LLMError(f"Connection refused: {conn_err}")
    except Exception as err:
        raise LLMError(f"OpenAI-compatible LLM request failed: {str(err)}")

class LLMError(RuntimeError):
    """The call did not complete. Distinct from the model answering 'no'."""


def provider() -> str | None:
    """Which cloud model is configured, if any."""
    preferred = os.getenv(PREFERRED_ENV)
    if preferred in PROVIDER_ENV and os.getenv(PROVIDER_ENV[preferred]):
        return preferred
    for name, env in PROVIDER_ENV.items():
        if os.getenv(env):
            return name
    return None


def available() -> bool:
    return provider() is not None


def complete(prompt: str, system: str = "", max_tokens: int = 1024) -> str | None:
    """
    Prompt in, text out. None only when no key is configured.

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
        return _gemini(requests, prompt, system, max_tokens)
    except LLMError:
        raise
    except Exception as e:                                  # noqa: BLE001
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
