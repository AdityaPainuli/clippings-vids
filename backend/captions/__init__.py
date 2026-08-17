"""
Bolcap caption engine — standalone, style-schema-driven subtitle generation.

Flow: transcribe (word timestamps) → optional Hinglish romanization →
style (JSON schema: fonts, colors, animation) → export (.ass / .srt /
burned MP4 / alpha overlay .mov for NLE editors).

Channel-agnostic: the same engine backs the SaaS API, the CLI, and any
future NLE plugin.
"""

from .styles import CaptionStyle, STYLE_PRESETS
from .transcribe import transcribe_video
from .romanize import romanize_words
from .engine import build_ass
from .render import export_srt, burn_video, render_overlay, probe_video
