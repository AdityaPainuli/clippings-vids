import pytest
from pydantic import ValidationError
from captions.styles import CaptionStyle, STYLE_PRESETS


def test_unknown_preset_keyerror():
    """Ensure accessing an invalid preset raises KeyError (which cli.py will catch)."""
    with pytest.raises(KeyError):
        _ = STYLE_PRESETS["bold_impect"]  # Typo of bold_impact


def test_extra_fields_forbidden_in_style():
    """Ensure typo'd keys in CaptionStyle raise ValidationError instead of being silently ignored."""
    with pytest.raises(ValidationError) as exc_info:
        CaptionStyle(fnot_size=90)  # Typo of font_size
    
    # Verify the error message mentions the misspelled field
    assert "fnot_size" in str(exc_info.value)


def test_invalid_hex_color_in_style():
    """Ensure invalid hex colors are caught by the field validator."""
    with pytest.raises(ValidationError) as exc_info:
        CaptionStyle(text_color="#GGGGGG")  # Invalid hex
    
    assert "text_color" in str(exc_info.value)
    assert "Invalid hex color" in str(exc_info.value)


def test_out_of_range_numeric_field():
    """Ensure out-of-range values (e.g., font_size > 200) are caught."""
    with pytest.raises(ValidationError) as exc_info:
        CaptionStyle(font_size=300)  # Max allowed is 200
    
    assert "font_size" in str(exc_info.value)
    assert "less than or equal to 200" in str(exc_info.value).lower()