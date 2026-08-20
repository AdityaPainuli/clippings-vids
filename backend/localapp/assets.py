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
from pathlib import Path

import requests

BOLCAP_HOME = Path(os.getenv("BOLCAP_HOME", Path.home() / ".bolcap"))
BIN_DIR = BOLCAP_HOME / "bin"
MODEL_DIR = BOLCAP_HOME / "models"
CONFIG_PATH = BOLCAP_HOME / "config.json"

DEFAULT_MODEL = os.getenv("BOLCAP_MODEL", "small")
MODEL_CHOICES = {
    # size label shown in the UI — honest speed expectations, per DECISIONS.md
    "small":  {"disk": "~500MB", "note": "Fast on any machine, decent Hinglish"},
    "medium": {"disk": "~1.5GB", "note": "Better accuracy, slow without GPU"},
    "large-v3": {"disk": "~3GB", "note": "Best accuracy, needs GPU or patience"},
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


# ── Config persistence ───────────────────────────────────────────────────────

def load_config() -> dict:
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def save_config(**updates):
    cfg = {**load_config(), **updates}
    BOLCAP_HOME.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=1)


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
