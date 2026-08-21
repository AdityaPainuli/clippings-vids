#!/usr/bin/env python3
"""
Measure filler detection against labelled fixtures.

Filler detection is the one part of Tighten that can silently destroy a
sentence, so it gets numbers rather than opinions. Run this after any
change to the word lists or the scoring weights.

Precision matters far more than recall here: cutting a real word leaves the
user hunting for a hole in their audio, while missing a filler costs them
one click. The gate below fails the run on ANY real word being cut.

    python scripts/eval_tighten.py
"""

import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from captions import tighten  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "tests", "fixtures", "*.json")


WORD_CUT_REASONS = ("filler", "repeat")   # silence cuts never target words


def word_cut_by(cuts, word, require_auto=True):
    """Was this word's core removed? (edge padding alone doesn't count)"""
    mid = (word["start"] + word["end"]) / 2
    return any(c.start <= mid < c.end and (c.auto or not require_auto)
               for c in cuts if c.reason in WORD_CUT_REASONS)


def evaluate(path, cfg):
    fx = json.load(open(path, encoding="utf-8"))
    words = fx["words"]
    cuts = tighten.detect(words, cfg, duration=fx["duration"])

    truth = set(fx.get("filler_indices", []))
    real = set(fx.get("known_real_words", []))

    tp = fn = fp = 0
    damaged = []
    for i, w in enumerate(words):
        cut = word_cut_by(cuts, w)
        if i in truth:
            tp += cut
            fn += not cut
        elif cut:
            fp += 1
            damaged.append((i, w["text"]))

    precision = tp / (tp + fp) if (tp + fp) else 1.0
    recall = tp / (tp + fn) if (tp + fn) else 1.0
    real_cut = [(i, t) for i, t in damaged if i in real]

    return {
        "name": fx["name"], "words": len(words),
        "tp": tp, "fp": fp, "fn": fn,
        "precision": precision, "recall": recall,
        "damaged": damaged, "real_words_cut": real_cut,
        "summary": tighten.summarize(cuts, fx["duration"]),
    }


def main():
    cfg = tighten.TightenConfig()
    results = [evaluate(p, cfg) for p in sorted(glob.glob(FIXTURES))]
    if not results:
        print("no fixtures found")
        return 1

    print(f"{'fixture':<34} {'words':>6} {'TP':>4} {'FP':>4} {'FN':>4} "
          f"{'prec':>6} {'recall':>7}")
    print("-" * 72)
    for r in results:
        print(f"{r['name']:<34} {r['words']:>6} {r['tp']:>4} {r['fp']:>4} "
              f"{r['fn']:>4} {r['precision']:>6.0%} {r['recall']:>7.0%}")

    print()
    for r in results:
        s = r["summary"]
        print(f"{r['name']}: would remove {s['removed_seconds']}s "
              f"({s['removed_percent']}%), {s['auto_cuts']} auto / "
              f"{s['suggested_cuts']} suggested")
        if r["damaged"]:
            print(f"   cut non-filler words: {r['damaged']}")

    failures = [r for r in results if r["real_words_cut"]]
    if failures:
        print("\nFAIL — real words were cut:")
        for r in failures:
            print(f"  {r['name']}: {r['real_words_cut']}")
        return 1

    total_recall = (sum(r["tp"] for r in results) /
                    max(1, sum(r["tp"] + r["fn"] for r in results)))
    print(f"\nPASS — no real word cut in any fixture. "
          f"Filler recall {total_recall:.0%}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
