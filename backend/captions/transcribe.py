"""
Word-level transcription with automatic backend selection.

Prefers mlx-whisper (Apple Silicon GPU, ~40x faster than CPU) and falls
back to openai-whisper. Both return the same word-timing shape:
[{"start": float, "end": float, "text": str}, ...]
"""

import os
import subprocess
import tempfile

MLX_MODEL = os.getenv("CAPTIONS_MLX_MODEL", "mlx-community/whisper-large-v3-turbo")
CPU_MODEL = os.getenv("CAPTIONS_CPU_MODEL", "small")

_cpu_model_cache = None


def _cpu_model():
    """Load the CPU whisper model once per process — loading dominates latency."""
    global _cpu_model_cache
    if _cpu_model_cache is None:
        import whisper
        _cpu_model_cache = whisper.load_model(CPU_MODEL)
    return _cpu_model_cache


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


def transcribe_video(video_path: str, language: str | None = None) -> dict:
    """
    Returns {"language": str, "backend": str, "words": [...]}.
    `language` forces a language code (e.g. "hi"); None auto-detects.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        audio = os.path.join(tmpdir, "audio.wav")
        _extract_audio(video_path, audio)

        try:
            import mlx_whisper
            result = mlx_whisper.transcribe(
                audio, path_or_hf_repo=MLX_MODEL,
                word_timestamps=True, language=language, verbose=None,
            )
            backend = f"mlx:{MLX_MODEL}"
        except ImportError:
            model = _cpu_model()
            result = model.transcribe(
                audio, word_timestamps=True, language=language, fp16=False, verbose=None,
            )
            backend = f"whisper:{CPU_MODEL}"

    return {
        "language": result.get("language"),
        "backend": backend,
        "words": _words_from_result(result),
    }
