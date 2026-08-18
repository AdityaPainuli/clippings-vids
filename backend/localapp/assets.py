"""
First-run asset management — ffmpeg + whisper models into ~/.bolcap.

Keeps the app download small: heavy pieces are fetched on first launch
with progress, resumable by re-running. System ffmpeg on PATH is always
preferred; the download only happens when there isn't one.

Manifest of download sources lives in FFMPEG_SOURCES — pinned per
platform at release time (checksums optional but honored when present).
"""

import hashlib
import os
import platform
import shutil
import stat
import subprocess
import tarfile
import zipfile
from pathlib import Path

import requests

BOLCAP_HOME = Path(os.getenv("BOLCAP_HOME", Path.home() / ".bolcap"))
BIN_DIR = BOLCAP_HOME / "bin"
MODEL_DIR = BOLCAP_HOME / "models"

DEFAULT_MODEL = os.getenv("BOLCAP_MODEL", "small")
MODEL_CHOICES = {
    # size label shown in the UI — honest speed expectations, per DECISIONS.md
    "small":  {"disk": "~500MB", "note": "Fast on any machine, decent Hinglish"},
    "medium": {"disk": "~1.5GB", "note": "Better accuracy, slow without GPU"},
    "large-v3": {"disk": "~3GB", "note": "Best accuracy, needs GPU or patience"},
}

FFMPEG_SOURCES = {
    # platform.system()-machine → {url, archive member containing the binary, sha256 (optional)}
    "Windows-AMD64": {
        "url": "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl.zip",
        "binary": "ffmpeg.exe",
    },
    "Linux-x86_64": {
        "url": "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-linux64-gpl.tar.xz",
        "binary": "ffmpeg",
    },
    # evermeet ships x86_64; Apple Silicon runs it under Rosetta. Fine as a
    # fallback — most Macs have brew ffmpeg on PATH anyway.
    "Darwin-arm64": {
        "url": "https://evermeet.cx/ffmpeg/getrelease/zip",
        "binary": "ffmpeg",
    },
    "Darwin-x86_64": {
        "url": "https://evermeet.cx/ffmpeg/getrelease/zip",
        "binary": "ffmpeg",
    },
}


def _platform_key() -> str:
    return f"{platform.system()}-{platform.machine()}"


# ── Status ───────────────────────────────────────────────────────────────────

def ffmpeg_path() -> str | None:
    """Resolved ffmpeg binary: PATH first, then the downloaded copy."""
    on_path = shutil.which("ffmpeg")
    if on_path:
        return on_path
    exe = "ffmpeg.exe" if platform.system() == "Windows" else "ffmpeg"
    local = BIN_DIR / exe
    return str(local) if local.exists() else None


def model_installed(model: str = DEFAULT_MODEL) -> bool:
    marker = MODEL_DIR / f".{model}.ready"
    return marker.exists()


def setup_status() -> dict:
    return {
        "ffmpeg": ffmpeg_path(),
        "ffmpeg_needed": ffmpeg_path() is None,
        "models": {m: model_installed(m) for m in MODEL_CHOICES},
        "model_choices": MODEL_CHOICES,
        "default_model": DEFAULT_MODEL,
        "home": str(BOLCAP_HOME),
    }


# ── Downloads ────────────────────────────────────────────────────────────────

def _stream_download(url: str, dest: Path, progress: dict, sha256: str | None = None):
    """Download with byte progress written into `progress` (shared dict)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    hasher = hashlib.sha256()
    with requests.get(url, stream=True, timeout=(10, 60), allow_redirects=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length") or 0)
        progress.update({"total_bytes": total, "done_bytes": 0})
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                hasher.update(chunk)
                progress["done_bytes"] += len(chunk)
    if sha256 and hasher.hexdigest() != sha256:
        dest.unlink(missing_ok=True)
        raise RuntimeError("Checksum mismatch — download corrupted, try again")


def _extract_binary(archive: Path, member_name: str, dest: Path):
    """Pull the ffmpeg binary out of a zip/tar archive, whatever the layout."""
    if archive.suffix == ".zip" or zipfile.is_zipfile(archive):
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
    source = FFMPEG_SOURCES.get(key)
    if not source:
        raise RuntimeError(
            f"No ffmpeg build known for {key} — install ffmpeg yourself and re-launch")

    progress["step"] = "ffmpeg"
    archive = BOLCAP_HOME / "tmp" / os.path.basename(source["url"].split("?")[0] or "ffmpeg.zip")
    _stream_download(source["url"], archive, progress, source.get("sha256"))

    exe = "ffmpeg.exe" if platform.system() == "Windows" else "ffmpeg"
    BIN_DIR.mkdir(parents=True, exist_ok=True)
    _extract_binary(archive, source["binary"], BIN_DIR / exe)
    archive.unlink(missing_ok=True)

    # Sanity check the binary actually runs
    r = subprocess.run([str(BIN_DIR / exe), "-version"], capture_output=True)
    if r.returncode != 0:
        raise RuntimeError("Downloaded ffmpeg failed to run")


def download_whisper_model(model: str, progress: dict):
    if model not in MODEL_CHOICES:
        raise RuntimeError(f"Unknown model {model}")
    progress["step"] = f"model:{model}"
    progress.update({"total_bytes": 0, "done_bytes": 0})  # HF handles its own progress
    from faster_whisper import download_model
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    download_model(model, cache_dir=str(MODEL_DIR))
    (MODEL_DIR / f".{model}.ready").touch()


def run_setup(model: str, progress: dict):
    """Full first-run setup: ffmpeg if missing, then the chosen model."""
    try:
        if ffmpeg_path() is None:
            download_ffmpeg(progress)
        if not model_installed(model):
            download_whisper_model(model, progress)
        progress["step"] = "done"
        progress["status"] = "ready"
    except Exception as e:
        progress["status"] = "failed"
        progress["error"] = str(e)[:500]


def apply_environment():
    """
    Make downloaded assets visible to the engine: prepend our bin dir to
    PATH (captions/* invoke 'ffmpeg' by name) and point faster-whisper at
    the model cache.
    """
    if ffmpeg_path() and str(BIN_DIR) not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{BIN_DIR}{os.pathsep}{os.environ.get('PATH', '')}"
    os.environ.setdefault("HF_HUB_CACHE", str(MODEL_DIR))
    os.environ.setdefault("CAPTIONS_CPU_MODEL", DEFAULT_MODEL)
