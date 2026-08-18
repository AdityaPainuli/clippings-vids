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

MLX_MODEL = os.getenv("CAPTIONS_MLX_MODEL", "mlx-community/whisper-large-v3-turbo")
CPU_MODEL = os.getenv("CAPTIONS_CPU_MODEL", "small")

_fw_model_cache = None
_cpu_model_cache = None


def _extract_audio(video_path: str, out_path: str):
    r = subprocess.run(
        ["ffmpeg", "-y", "-i", video_path, "-ac", "1", "-ar", "16000", "-vn", out_path],
        capture_output=True, text=True,
    )
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
    )
    return {"language": result.get("language"),
            "backend": f"mlx:{MLX_MODEL}",
            "words": _words_from_result(result)}


def _faster_whisper_model():
    """Load once per process; picks CUDA float16 when available, else int8 CPU."""
    global _fw_model_cache
    if _fw_model_cache is None:
        from faster_whisper import WhisperModel
        try:
            _fw_model_cache = WhisperModel(CPU_MODEL, device="cuda", compute_type="float16")
        except Exception:
            _fw_model_cache = WhisperModel(CPU_MODEL, device="cpu", compute_type="int8")
    return _fw_model_cache


def _transcribe_faster_whisper(audio: str, language: str | None) -> dict | None:
    try:
        from faster_whisper import WhisperModel  # noqa: F401
    except ImportError:
        return None
    model = _faster_whisper_model()
    segments, info = model.transcribe(audio, word_timestamps=True, language=language)
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
            "backend": f"faster-whisper:{CPU_MODEL}",
            "words": words or [s for s in sentence_fallback if s["text"]]}


def _transcribe_openai_whisper(audio: str, language: str | None) -> dict:
    global _cpu_model_cache
    import whisper
    if _cpu_model_cache is None:
        _cpu_model_cache = whisper.load_model(CPU_MODEL)
    result = _cpu_model_cache.transcribe(
        audio, word_timestamps=True, language=language, fp16=False, verbose=None,
    )
    return {"language": result.get("language"),
            "backend": f"whisper:{CPU_MODEL}",
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
        return result
