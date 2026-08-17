"""
Caption style schema — the customization surface exposed to users.

Users edit colors as hex (#RRGGBB or #RRGGBBAA); conversion to ASS
&HAABBGGRR happens here so no ASS knowledge leaks out of the engine.
"""

import re
from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator

HEX_RE = re.compile(r"^#?([0-9a-fA-F]{6})([0-9a-fA-F]{2})?$")


def hex_to_ass(hex_color: str) -> str:
    """'#RRGGBB' or '#RRGGBBAA' → ASS '&HAABBGGRR' (AA: 00=opaque, FF=transparent)."""
    m = HEX_RE.match(hex_color)
    if not m:
        raise ValueError(f"Invalid hex color: {hex_color}")
    rgb, alpha = m.group(1), m.group(2)
    r, g, b = rgb[0:2], rgb[2:4], rgb[4:6]
    # CSS alpha (FF=opaque) → ASS alpha (00=opaque)
    aa = f"{255 - int(alpha, 16):02X}" if alpha else "00"
    return f"&H{aa}{b.upper()}{g.upper()}{r.upper()}"


class Animation(BaseModel):
    type: Literal["none", "pop", "fade", "karaoke"] = "none"
    # pop: word scales scale_start → scale_end → 100
    scale_start: int = Field(50, ge=10, le=100)
    scale_end: int = Field(115, ge=100, le=200)
    # pop/fade: how long the entrance runs
    duration_ms: int = Field(150, ge=50, le=1000)


class CaptionStyle(BaseModel):
    """Everything an editor can customize about how captions look."""
    font: str = "Arial Black"
    font_size: int = Field(72, ge=24, le=200)
    highlight_scale: float = Field(1.12, ge=1.0, le=1.5)   # active-word size boost
    uppercase: bool = True
    words_per_line: int = Field(3, ge=1, le=8)

    text_color: str = "#FFFFFF"
    highlight_color: str = "#FFFF00"
    dim_alpha: int = Field(0xAA, ge=0, le=255)             # future-word transparency
    outline_color: str = "#000000"
    shadow_color: str = "#00000099"
    outline_width: int = Field(4, ge=0, le=10)
    shadow_width: int = Field(3, ge=0, le=10)

    # 1-9 numpad alignment (2 = bottom-center); margin_v in PlayRes pixels
    alignment: int = Field(2, ge=1, le=9)
    margin_v: int = Field(320, ge=0, le=1920)

    animation: Animation = Animation()

    @field_validator("text_color", "highlight_color", "outline_color", "shadow_color")
    @classmethod
    def _valid_hex(cls, v):
        if not HEX_RE.match(v):
            raise ValueError(f"Invalid hex color: {v}")
        return v

    # ASS-space accessors used by the engine
    @property
    def ass_text_color(self): return hex_to_ass(self.text_color)
    @property
    def ass_highlight_color(self): return hex_to_ass(self.highlight_color)
    @property
    def ass_outline_color(self): return hex_to_ass(self.outline_color)
    @property
    def ass_shadow_color(self): return hex_to_ass(self.shadow_color)
    @property
    def ass_dim_alpha(self): return f"&H{self.dim_alpha:02X}"


# Built-in presets — starting points users can fork and tweak
STYLE_PRESETS: dict[str, CaptionStyle] = {
    "default": CaptionStyle(),
    "bold_impact": CaptionStyle(
        font_size=80, outline_width=5,
        animation=Animation(type="pop", scale_start=50, scale_end=115, duration_ms=150),
    ),
    "subtle": CaptionStyle(
        font="Inter", font_size=56, uppercase=False, words_per_line=4,
        highlight_color="#88FF88", outline_width=2, shadow_width=1,
        shadow_color="#00000044", margin_v=300,
        animation=Animation(type="fade", duration_ms=200),
    ),
    "karaoke": CaptionStyle(
        words_per_line=5, highlight_color="#FF5500", shadow_width=2,
        animation=Animation(type="karaoke"),
    ),
}
