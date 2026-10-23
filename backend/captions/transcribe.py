"""
Word-level transcription with automatic backend selection.

Backend priority (see DECISIONS.md):
  1. mlx-whisper     — Apple Silicon GPU, fastest by far
  2. faster-whisper  — CTranslate2: no PyTorch, int8 CPU, CUDA auto-detect;
                       the portable backend that makes Bolcap cross-platform
  3. openai-whisper  — legacy fallback

All backends return the same shape:
{"language": str, "backend": str, "words": [{"start", "end", "text"}, ...]}
"""

import os
import subprocess
import tempfile
import threading

MLX_MODEL = os.getenv("CAPTIONS_MLX_MODEL", "mlx-community/whisper-large-v3-turbo")


def _cpu_model_name() -> str:
    """Read at call time, not import time — the local app changes the
    active model after setup without restarting the process."""
    return os.getenv("CAPTIONS_CPU_MODEL", "small")


_fw_model_cache = None      # (name, model)
_cpu_model_cache = None     # (name, model)
# Loading a model is hundreds of MB of allocation. Two uploads arriving close
# together must not both construct one and exhaust memory, so the load itself
# is serialized rather than just the cache check.
_fw_model_lock = threading.Lock()
_cpu_model_lock = threading.Lock()


# Extracting 16 kHz mono runs far faster than realtime, so one second of
# budget per second of media plus a floor is generous. The fixed cap covers
# files ffprobe cannot read a duration from. The ceiling exists because the
# duration is least trustworthy on exactly the broken files this guards
# against (MPEG-TS timestamp wraparound reports ~95443s, about 26h).
_PROBE_TIMEOUT = 30
_EXTRACT_MIN_TIMEOUT = 120
_EXTRACT_FALLBACK_TIMEOUT = 1800
_EXTRACT_MAX_TIMEOUT = 4 * 3600


def _media_duration(path: str) -> float | None:
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True, timeout=_PROBE_TIMEOUT,
        )
        return float(r.stdout.strip()) if r.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return None


def _extract_timeout(video_path: str) -> float:
    duration = _media_duration(video_path)
    if duration is None or duration <= 0:
        return _EXTRACT_FALLBACK_TIMEOUT
    return min(_EXTRACT_MIN_TIMEOUT + duration, _EXTRACT_MAX_TIMEOUT)


def _extract_audio(video_path: str, out_path: str):
    timeout = _extract_timeout(video_path)
    try:
        r = subprocess.run(
            ["ffmpeg", "-y", "-i", video_path, "-ac", "1", "-ar", "16000", "-vn", out_path],
            capture_output=True, text=True, timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Audio extraction timed out after {timeout:.0f}s") from None
    if r.returncode != 0:
        raise RuntimeError(f"Audio extraction failed: {r.stderr[-300:]}")


def _words_from_result(result: dict) -> list:
    words = []
    for seg in result.get("segments", []):
        for w in seg.get("words", []):
            text = w["word"].strip()
            if text:
                words.append({"start": float(w["start"]), "end": float(w["end"]), "text": text})
    # Fallback to sentence segments if the model gave no word timings
    if not words:
        for seg in result.get("segments", []):
            text = seg["text"].strip()
            if text:
                words.append({"start": float(seg["start"]), "end": float(seg["end"]), "text": text})
    return words


# ── Backends ─────────────────────────────────────────────────────────────────

def _transcribe_mlx(audio: str, language: str | None) -> dict | None:
    try:
        import mlx_whisper
    except ImportError:
        return None
    result = mlx_whisper.transcribe(
        audio, path_or_hf_repo=MLX_MODEL,
        word_timestamps=True, language=language, verbose=None,
        # Whisper loops on the same phrase when audio is quiet or noisy;
        # not feeding it its own previous output breaks the loop.
        condition_on_previous_text=False,
    )
    return {"language": result.get("language"),
            "backend": f"mlx:{MLX_MODEL}",
            "words": _words_from_result(result)}


def _faster_whisper_model():
    """Cached per model name; picks CUDA float16 when available, else int8 CPU."""
    global _fw_model_cache
    name = _cpu_model_name()
    with _fw_model_lock:
        if _fw_model_cache is None or _fw_model_cache[0] != name:
            from faster_whisper import WhisperModel
            try:
                model = WhisperModel(name, device="cuda", compute_type="float16")
            except Exception:
                model = WhisperModel(name, device="cpu", compute_type="int8")
            _fw_model_cache = (name, model)
        return _fw_model_cache[1]


def _transcribe_faster_whisper(audio: str, language: str | None) -> dict | None:
    try:
        from faster_whisper import WhisperModel  # noqa: F401
    except ImportError:
        return None
    model = _faster_whisper_model()
    segments, info = model.transcribe(
        audio, word_timestamps=True, language=language,
        # Same loop guard as the mlx path, plus VAD so silence never gets
        # a chance to hallucinate text in the first place.
        condition_on_previous_text=False, vad_filter=True,
    )
    words = []
    sentence_fallback = []
    for seg in segments:  # generator — transcription happens during iteration
        sentence_fallback.append({"start": float(seg.start), "end": float(seg.end),
                                  "text": seg.text.strip()})
        for w in (seg.words or []):
            text = w.word.strip()
            if text:
                words.append({"start": float(w.start), "end": float(w.end), "text": text})
    return {"language": info.language,
            "backend": f"faster-whisper:{_cpu_model_name()}",
            "words": words or [s for s in sentence_fallback if s["text"]]}


def _transcribe_openai_whisper(audio: str, language: str | None) -> dict:
    global _cpu_model_cache
    import whisper
    name = _cpu_model_name()
    with _cpu_model_lock:
        if _cpu_model_cache is None or _cpu_model_cache[0] != name:
            _cpu_model_cache = (name, whisper.load_model(name))
        model = _cpu_model_cache[1]
    result = model.transcribe(
        audio,
        word_timestamps=True,
        language=language,
        fp16=False,
        verbose=None,
        # Keep the legacy backend from feeding its own output back into the
        # next decode window, which can amplify repeated-text hallucinations.
        condition_on_previous_text=False,
        # Keep Whisper's built-in silence / low-confidence / repetition
        # rejection enabled explicitly instead of relying on library defaults.
        no_speech_threshold=0.6,
        logprob_threshold=-1.0,
        compression_ratio_threshold=2.4,
    )
    return {"language": result.get("language"),
            "backend": f"whisper:{name}",
            "words": _words_from_result(result)}


def transcribe_video(video_path: str, language: str | None = None) -> dict:
    """
    Returns {"language": str, "backend": str, "words": [...]}.
    `language` forces a language code (e.g. "hi"); None auto-detects.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        audio = os.path.join(tmpdir, "audio.wav")
        _extract_audio(video_path, audio)

        result = _transcribe_mlx(audio, language)
        if result is None:
            result = _transcribe_faster_whisper(audio, language)
        if result is None:
            result = _transcribe_openai_whisper(audio, language)
        if result and "words" in result:
            from .lang_detect import tag_words
            result["words"] = tag_words(result["words"], result.get("language"))
        return result
