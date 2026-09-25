import pytest
from pydantic import ValidationError

from captions.styles import CaptionStyle


def assert_font_rejected(font: str, message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        CaptionStyle(font=font)


def test_font_with_comma_is_rejected():
    assert_font_rejected("Arial, Black", "font must not contain commas or line breaks")


def test_font_with_newline_is_rejected():
    assert_font_rejected("Arial\nBlack", "font must not contain commas or line breaks")


def test_font_with_carriage_return_is_rejected():
    assert_font_rejected("Arial\rBlack", "font must not contain commas or line breaks")


def test_overlong_font_is_rejected():
    assert_font_rejected("A" * 101, "at most 100 characters")


def test_font_with_leading_whitespace_is_rejected():
    assert_font_rejected(" Arial Black", "leading or trailing whitespace")


def test_font_with_trailing_whitespace_is_rejected():
    assert_font_rejected("Arial Black ", "leading or trailing whitespace")


def test_valid_font_is_preserved():
    style = CaptionStyle(font="Arial Black")
    assert style.font == "Arial Black"


def test_valid_font_with_internal_spaces_is_preserved():
    style = CaptionStyle(font="Noto Sans Devanagari")
    assert style.font == "Noto Sans Devanagari"
