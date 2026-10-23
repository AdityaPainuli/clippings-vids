import unittest
from unittest import mock

from captions import retakes, tighten


def _words(count, start, step=0.2):
    return [{"start": round(start + i * step, 2),
             "end": round(start + i * step + 0.1, 2),
             "text": f"w{i}"} for i in range(count)]


class RetakePauseBoundaryTest(unittest.TestCase):
    def test_half_second_pause_splits_phrases(self):
        words = _words(6, 0.0, step=0.18) + _words(6, 1.5)
        phrases = retakes.split_phrases(words, [(1.0, 1.5)])
        self.assertEqual(len(phrases), 2)
        self.assertEqual(phrases[1].start, 1.5)

    def test_padded_span_would_hide_the_same_pause(self):
        # The cutter's padding (0.12s per side) turns 0.50s into 0.26s, which
        # is below MIN_PAUSE. This is the old input retakes was using.
        words = _words(6, 0.0, step=0.18) + _words(6, 1.5)
        phrases = retakes.split_phrases(words, [(1.12, 1.38)])
        self.assertEqual(len(phrases), 1)

    def test_detect_asks_for_unpadded_silences(self):
        seen = {}

        def fake_detect(path, cfg):
            seen["cfg"] = cfg
            return [tighten.Cut(1.0, 1.5, "silence", 0.9, auto=True)]

        with mock.patch("captions.tighten.detect_silences_from_audio",
                        side_effect=fake_detect):
            retakes.detect(_words(12, 0.0), media_path="clip.mp4",
                           complete=lambda *a, **k: '{"retake": false}')
        self.assertEqual(seen["cfg"].keep_padding, 0)


if __name__ == "__main__":
    unittest.main()
