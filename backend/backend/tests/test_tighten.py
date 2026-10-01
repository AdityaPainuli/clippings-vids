from captions.tighten import TightenConfig, detect_fillers


def test_hun_is_not_detected_as_filler():
    words = [
        {"start": 0.0, "end": 0.3, "text": "मैं"},
        {"start": 0.3, "end": 0.6, "text": "एक"},
        {"start": 0.6, "end": 1.0, "text": "student"},
        {"start": 1.0, "end": 1.4, "text": "हूँ"},
    ]

    cuts = detect_fillers(words, TightenConfig())

    assert not any(c.text == "हूँ" for c in cuts)


def test_nonlexical_hmm_is_still_detected_as_filler():
    words = [
        {"start": 0.0, "end": 0.3, "text": "hello"},
        {"start": 1.0, "end": 1.4, "text": "hmm"},
        {"start": 2.0, "end": 2.4, "text": "yes"},
    ]

    cuts = detect_fillers(words, TightenConfig())

    assert any(c.text == "hmm" for c in cuts)
