"""
First-run asset management — ffmpeg/ffprobe + whisper models into ~/.bolcap.

Keeps the app download small: heavy pieces are fetched on first launch
with byte progress and are resumable (Range + .part files). System
ffmpeg/ffprobe on PATH are always preferred; downloads only happen when
they're missing.

All download sources are pinned to immutable versioned artifacts with
sha256 hashes verified at packaging time — a missing or wrong hash fails
closed, nothing unverified is ever installed.
"""

import hashlib
import json
import os
import platform
import shutil
import stat
import subprocess
import tarfile
import threading
import time
import zipfile
import tempfile
from pathlib import Path

import requests

from captions import llm

BOLCAP_HOME = Path(os.getenv("BOLCAP_HOME", Path.home() / ".bolcap"))
BIN_DIR = BOLCAP_HOME / "bin"
MODEL_DIR = BOLCAP_HOME / "models"
CONFIG_PATH = BOLCAP_HOME / "config.json"

# medium is the default: small garbles Hindi badly enough to undercut the
# whole point of the product (measured on the reference clip — small gave
# "kisi bitar ka visakar", medium/large give "kisi bhi tarah ka avishkar").
DEFAULT_MODEL = os.getenv("BOLCAP_MODEL", "medium")
MODEL_CHOICES = {
    # size label shown in the UI — honest speed expectations, per DECISIONS.md
    "small":  {"disk": "~500MB", "note": "Fastest, but rough on Hindi — fine for English"},
    "medium": {"disk": "~1.5GB", "note": "Recommended for Hinglish. Slow on CPU-only"},
    "large-v3": {"disk": "~3GB", "note": "Best accuracy, needs a GPU or patience"},
}
# Allowed for CI/testing but not shown in the UI
_HIDDEN_MODELS = {"tiny"}

_BTBN = ("https://github.com/BtbN/FFmpeg-Builds/releases/download/"
         "autobuild-2026-08-18-15-03")

# platform.system()-machine → list of pinned artifacts, each providing
# one or more binaries. sha256 values computed from the artifacts at
# pin time; _stream_download refuses to install without a matching hash.
FFMPEG_SOURCES = {
    "Windows-AMD64": [{
        "url": f"{_BTBN}/ffmpeg-n8.1.2-44-g7c533d0f86-win64-gpl-8.1.zip",
        "binaries": ["ffmpeg.exe", "ffprobe.exe"],
        "sha256": "66e3797adad33063ae3f55c7eacb9f1bff604322a4e50225039626230fd0c0d1",
    }],
    "Linux-x86_64": [{
        "url": f"{_BTBN}/ffmpeg-n8.1.2-44-g7c533d0f86-linux64-gpl-8.1.tar.xz",
        "binaries": ["ffmpeg", "ffprobe"],
        "sha256": "03ccc8a1cb534b97c2bc43f322ddb1b7c23bd325abb7e4c31aa37f4b4c0e648f",
    }],
    # evermeet ships x86_64; Apple Silicon runs it under Rosetta. Fine as a
    # fallback — most Macs have brew ffmpeg on PATH anyway.
    "Darwin-arm64": [
        {"url": "https://evermeet.cx/ffmpeg/ffmpeg-9.0.1.zip",
         "binaries": ["ffmpeg"],
         "sha256": "8a8c9e549983409fe6604b9aa665648b7a5def9407fe814c39c8b2ea7f64a48f"},
        {"url": "https://evermeet.cx/ffmpeg/ffprobe-9.0.1.zip",
         "binaries": ["ffprobe"],
         "sha256": "d13f35db03456b7f65b7edb6437c86e23810fbfe91795e571f5b77211343b4f1"},
    ],
}
FFMPEG_SOURCES["Darwin-x86_64"] = FFMPEG_SOURCES["Darwin-arm64"]


def _platform_key() -> str:
    return f"{platform.system()}-{platform.machine()}"


# ── Platform requirements ────────────────────────────────────────────────────
#
# These are not aspirations, they are measured from the shipped binaries. The
# release builds on GitHub's current runners, and the wheels they pull in set
# a floor:
#
#   macOS   numpy and onnxruntime are built with minos 14.0, on both arm64 and
#           x86_64. Everything else in the bundle goes back to 11.0.
#   Linux   the bundled CPython needs GLIBC_2.38, and libmvec 2.39 — so
#           Ubuntu 24.04, Debian 13, Fedora 39, or newer.
#
# The reason this needs checking rather than documenting: faster-whisper (and
# therefore numpy) is imported lazily, at transcribe time. On an older OS the
# app started, opened, accepted a video and downloaded a 1.5GB model before
# dying — it looked like it worked right up to the part that mattered.

MIN_MACOS = (14, 0)
MIN_GLIBC = (2, 38)


def unmet_requirement() -> str | None:
    """A plain sentence about why this machine cannot run Bolcap, or None."""
    system = platform.system()
    if system == "Darwin":
        release = platform.mac_ver()[0]
        parts = [int(n) for n in release.split(".")[:2] if n.isdigit()]
        if parts and tuple(parts + [0])[:2] < MIN_MACOS:
            return (f"Bolcap needs macOS {MIN_MACOS[0]} (Sonoma) or later. "
                    f"This Mac is on {release}.")
    elif system == "Linux":
        try:
            version = os.confstr("CS_GNU_LIBC_VERSION") or ""
        except (ValueError, OSError):
            return None
        parts = [int(n) for n in version.split()[-1].split(".")[:2] if n.isdigit()]
        if len(parts) == 2 and tuple(parts) < MIN_GLIBC:
            return (f"Bolcap needs glibc {MIN_GLIBC[0]}.{MIN_GLIBC[1]} or later "
                    f"(Ubuntu 24.04, Debian 13, Fedora 39). "
                    f"This system has {'.'.join(str(n) for n in parts)}.")
    return None


def preflight() -> dict:
    """
    Load the native libraries now rather than at transcribe time.

    The OS check above catches the known cases with a message worth reading;
    importing anyway catches the ones nobody has met yet.
    """
    problem = unmet_requirement()
    if problem:
        return {"ok": False, "detail": problem}
    try:
        import numpy            # noqa: F401
        import ctranslate2      # noqa: F401
    except Exception as e:      # noqa: BLE001
        return {"ok": False,
                "detail": f"This build cannot load its transcription libraries "
                          f"on this system ({type(e).__name__}: {e})."}
    return {"ok": True, "detail": None}


# ── Config persistence ───────────────────────────────────────────────────────

def load_config() -> dict:
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def save_config(**updates):
    """
    Write the config so an API key inside it is never briefly readable.

    Writing the file and then chmod-ing it leaves a window where the default
    umask has already put the key on disk as 0644 — and a write that fails
    never reaches the chmod at all. mkstemp creates at 0600, so the secret is
    owner-only from the moment it exists, and os.replace swaps it in
    atomically: a crash mid-write leaves the old config, never a truncated one.
    """
    cfg = {**load_config(), **updates}
    BOLCAP_HOME.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(BOLCAP_HOME, 0o700)
    except OSError:
        pass

    fd, tmp = tempfile.mkstemp(dir=str(BOLCAP_HOME), prefix=".config-",
                               suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=1)
        os.replace(tmp, CONFIG_PATH)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ── Cloud model key ──────────────────────────────────────────────────────────
#
# Retake detection is the one feature that needs a cloud model. It used to read
# the key from the environment only, which is unreachable for the way Bolcap
# actually ships: a double-clicked app inherits launchd's environment on macOS,
# not the shell's, so `export ANTHROPIC_API_KEY=...` in a terminal never got
# there and the feature silently never appeared.
#
# The key is stored here and pushed into the environment at startup, which
# keeps captions/llm.py reading nothing but os.environ and keeps the engine
# layer unaware of the app's config.

KEY_ENV = llm.PROVIDER_ENV

# Captured at import, before apply_environment() injects anything. Afterwards
# every key looks like an environment key, so without this snapshot the app
# cannot tell which credential the user actually chose — it reported the wrong
# one in the UI and let the endpoint overwrite a deliberate override.
ENV_PROVIDED = {p: e for p, e in KEY_ENV.items() if os.environ.get(e)}


def env_provided(provider: str) -> bool:
    """Did this key come from the launch environment rather than the config?"""
    return provider in ENV_PROVIDED


def save_api_key(provider: str, key: str):
    """Persist a key, or clear it when `key` is empty."""
    if provider not in KEY_ENV:
        raise ValueError(f"unknown provider {provider!r}")
    if env_provided(provider):
        raise PermissionError(
            f"{KEY_ENV[provider]} is set in this app's environment; "
            "change it there instead.")
    keys = {**load_config().get("api_keys", {})}
    key = (key or "").strip()
    if key:
        keys[provider] = key
    else:
        keys.pop(provider, None)
        os.environ.pop(KEY_ENV[provider], None)
    save_config(api_keys=keys)
    apply_environment()


def api_key_status() -> dict:
    """
    What is configured, never the key itself.

    `source` matters to the user: a key set in the environment cannot be
    cleared from the UI, so the UI has to stop offering to.
    """
    stored = load_config().get("api_keys", {})
    out = {}
    for provider, env in KEY_ENV.items():
        if env_provided(provider):
            # The environment value is the one in use even when a key is also
            # saved, so it is the one described. Reporting the saved key's
            # hint here named a credential that was not being used.
            out[provider] = {"configured": True, "source": "environment",
                             "hint": _hint(os.environ[env]), "editable": False}
        elif stored.get(provider):
            out[provider] = {"configured": True, "source": "saved",
                             "hint": _hint(stored[provider]), "editable": True}
        else:
            out[provider] = {"configured": False, "source": None,
                             "hint": None, "editable": True}
    return out


def _hint(key: str) -> str:
    """Enough to recognise which key it is, not enough to use it."""
    key = key.strip()
    return f"…{key[-4:]}" if len(key) > 8 else "…"


# ── Status ───────────────────────────────────────────────────────────────────

def _binary_path(name: str) -> str | None:
    """Resolved binary: PATH first, then the downloaded copy."""
    on_path = shutil.which(name)
    if on_path:
        return on_path
    exe = f"{name}.exe" if platform.system() == "Windows" else name
    local = BIN_DIR / exe
    return str(local) if local.exists() else None


def ffmpeg_path() -> str | None:
    return _binary_path("ffmpeg")


def ffprobe_path() -> str | None:
    return _binary_path("ffprobe")


def ffmpeg_ready() -> bool:
    """Transcription runs ffmpeg AND ffprobe — both must resolve."""
    return ffmpeg_path() is not None and ffprobe_path() is not None


def model_installed(model: str) -> bool:
    return (MODEL_DIR / f".{model}.ready").exists()


def active_model() -> str:
    return load_config().get("model", DEFAULT_MODEL)


def _mlx_available() -> bool:
    """mlx-whisper (dev installs on Apple Silicon) needs no downloaded model."""
    try:
        import mlx_whisper  # noqa: F401
        return True
    except ImportError:
        return False


def setup_status() -> dict:
    return {
        "ffmpeg": ffmpeg_path(),
        "ffprobe": ffprobe_path(),
        "ffmpeg_needed": not ffmpeg_ready(),
        "models": {m: model_installed(m) for m in MODEL_CHOICES},
        "model_choices": MODEL_CHOICES,
        "default_model": active_model(),
        "mlx": _mlx_available(),
        "home": str(BOLCAP_HOME),
    }


# ── Downloads ────────────────────────────────────────────────────────────────

def _stream_download(url: str, dest: Path, progress: dict, sha256: str):
    """
    Resumable, checksummed download. Bytes accumulate in dest.part and
    survive interruption; a re-run resumes with a Range request. The file
    only lands at `dest` after the hash matches — fail closed, always.
    """
    if not sha256:
        raise RuntimeError(f"No pinned checksum for {url} — refusing to install")

    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")
    offset = part.stat().st_size if part.exists() else 0

    headers = {"Range": f"bytes={offset}-"} if offset else {}
    with requests.get(url, stream=True, timeout=(10, 60),
                      allow_redirects=True, headers=headers) as r:
        if offset and r.status_code != 206:
            # Server ignored the Range — start over
            offset = 0
            part.unlink(missing_ok=True)
            r.raise_for_status()
        elif not offset:
            r.raise_for_status()

        remaining = int(r.headers.get("Content-Length") or 0)
        progress.update({"total_bytes": offset + remaining, "done_bytes": offset})
        with open(part, "ab" if offset else "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                progress["done_bytes"] += len(chunk)

    hasher = hashlib.sha256()
    with open(part, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            hasher.update(chunk)
    if hasher.hexdigest() != sha256:
        part.unlink(missing_ok=True)
        raise RuntimeError("Checksum mismatch — download corrupted, try again")
    part.replace(dest)


def _extract_binary(archive: Path, member_name: str, dest: Path):
    """Pull one binary out of a zip/tar archive, whatever the layout."""
    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as z:
            member = next((m for m in z.namelist()
                           if m.split("/")[-1] == member_name), None)
            if not member:
                raise RuntimeError(f"{member_name} not found in archive")
            with z.open(member) as src, open(dest, "wb") as out:
                shutil.copyfileobj(src, out)
    else:
        with tarfile.open(archive) as t:
            member = next((m for m in t.getmembers()
                           if m.name.split("/")[-1] == member_name), None)
            if not member:
                raise RuntimeError(f"{member_name} not found in archive")
            with t.extractfile(member) as src, open(dest, "wb") as out:
                shutil.copyfileobj(src, out)
    dest.chmod(dest.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


def download_ffmpeg(progress: dict):
    key = _platform_key()
    sources = FFMPEG_SOURCES.get(key)
    if not sources:
        raise RuntimeError(
            f"No ffmpeg build known for {key} — install ffmpeg + ffprobe "
            "yourself and re-launch")

    BIN_DIR.mkdir(parents=True, exist_ok=True)
    suffix = ".exe" if platform.system() == "Windows" else ""
    for source in sources:
        progress["step"] = "ffmpeg"
        archive = BOLCAP_HOME / "tmp" / os.path.basename(source["url"].split("?")[0])
        _stream_download(source["url"], archive, progress, source.get("sha256", ""))
        for binary in source["binaries"]:
            _extract_binary(archive, binary, BIN_DIR / binary)
        archive.unlink(missing_ok=True)

    # Sanity check both binaries actually run
    for name in (f"ffmpeg{suffix}", f"ffprobe{suffix}"):
        r = subprocess.run([str(BIN_DIR / name), "-version"], capture_output=True)
        if r.returncode != 0:
            raise RuntimeError(f"Downloaded {name} failed to run")


def _model_total_bytes(model: str) -> int:
    """Total download size from the HF API; 0 when unavailable (offline check
    happens in the download itself)."""
    try:
        from faster_whisper.utils import _MODELS
        from huggingface_hub import HfApi
        repo = _MODELS.get(model, f"Systran/faster-whisper-{model}")
        info = HfApi().model_info(repo, files_metadata=True)
        return sum(s.size or 0 for s in info.siblings)
    except Exception:
        return 0


def download_whisper_model(model: str, progress: dict):
    if model not in MODEL_CHOICES and model not in _HIDDEN_MODELS:
        raise RuntimeError(f"Unknown model {model}")
    progress["step"] = f"model:{model}"
    progress.update({"total_bytes": _model_total_bytes(model), "done_bytes": 0})
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    # HF downloads land in the cache dir; a watcher thread turns growing
    # file sizes into byte progress for the UI.
    stop = threading.Event()

    def _watch():
        while not stop.is_set():
            try:
                progress["done_bytes"] = sum(
                    f.stat().st_size for f in MODEL_DIR.rglob("*") if f.is_file())
            except OSError:
                pass
            time.sleep(0.5)

    watcher = threading.Thread(target=_watch, daemon=True)
    watcher.start()
    try:
        from faster_whisper import download_model
        download_model(model, cache_dir=str(MODEL_DIR))
    finally:
        stop.set()
        watcher.join(timeout=2)
    (MODEL_DIR / f".{model}.ready").touch()


def run_setup(model: str, progress: dict):
    """Full first-run setup: ffmpeg/ffprobe if missing, then the chosen model."""
    try:
        if not ffmpeg_ready():
            download_ffmpeg(progress)
        if not model_installed(model):
            download_whisper_model(model, progress)
        save_config(model=model)
        progress["step"] = "done"
        progress["status"] = "ready"
    except Exception as e:
        progress["status"] = "failed"
        progress["error"] = str(e)[:500]


def apply_environment():
    """
    Make installed assets visible to the engine: prepend our bin dir to
    PATH (captions/* invoke ffmpeg/ffprobe by name), point faster-whisper
    at the model cache, select the persisted model, and expose the
    bundled fonts dir for libass.
    """
    if str(BIN_DIR) not in os.environ.get("PATH", "") and BIN_DIR.exists():
        os.environ["PATH"] = f"{BIN_DIR}{os.pathsep}{os.environ.get('PATH', '')}"
    os.environ.setdefault("HF_HUB_CACHE", str(MODEL_DIR))
    os.environ["CAPTIONS_CPU_MODEL"] = active_model()

    fonts = Path(__file__).parent / "fonts"
    if fonts.is_dir():
        os.environ.setdefault("CAPTIONS_FONTS_DIR", str(fonts))

    # A key from the launch environment wins — CI, the CLI, and anyone running
    # from a shell set it deliberately. Saved keys fill in the gaps only.
    #
    # Filling a gap can still change the answer: with GOOGLE_API_KEY exported
    # and an Anthropic key saved, injecting the saved one made provider() pick
    # Anthropic, quietly ignoring the provider the user chose. So the
    # environment's choice is recorded and honoured explicitly.
    for provider, value in (load_config().get("api_keys") or {}).items():
        env = KEY_ENV.get(provider)
        if env and value and not env_provided(provider):
            os.environ[env] = value

    for provider in KEY_ENV:
        if env_provided(provider):
            os.environ[llm.PREFERRED_ENV] = provider
            break
