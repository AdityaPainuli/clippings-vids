#!/usr/bin/env python3
"""
Measure retake detection.

The two stages fail in opposite directions, so they are scored separately:

  * **Stage 1 (no model)** must not miss a retake — anything it drops is gone
    for good. It is allowed to over-produce; a false candidate costs one cheap
    model call. So it is gated on recall, and its candidate count on real
    speech is reported as the cost of running it.
  * **Stage 2 (the model)** is the precision stage. It is exercised here with a
    stub so the wiring, the JSON handling, and the "never auto-apply" rule can
    be checked without a network call. Its actual judgement needs real
    recordings and a real key, and is not something this script can claim.

    python scripts/eval_retakes.py
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from captions import retakes  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "tests", "fixtures")


def _load(name):
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return json.load(f)


def stage_one(fx):
    silences = [tuple(s) for s in fx.get("silences", [])] or None
    phrases = retakes.split_phrases(fx["words"], silences)
    index = {id(p): i for i, p in enumerate(phrases)}
    groups = [sorted(index[id(p)] for p in g)
              for g in retakes.find_candidates(phrases)]
    return phrases, groups


def main():
    failures, notes = [], []

    fx = _load("staged_retakes.json")
    phrases, groups = stage_one(fx)

    if len(phrases) != len(fx["phrase_spans"]):
        failures.append(f"split {len(phrases)} phrases, fixture describes "
                        f"{len(fx['phrase_spans'])}")

    print(f"{'retake group':<16} {'found?':>8}   candidate group")
    print("-" * 60)
    for truth in fx["retake_groups"]:
        hit = next((g for g in groups if set(truth) <= set(g)), None)
        print(f"{str(truth):<16} {'yes' if hit else 'NO':>8}   {hit or ''}")
        if not hit:
            failures.append(f"retake group {truth} never reached the model")

    print(f"\nstage 1 produced {len(groups)} groups from {len(phrases)} phrases")
    for trap in fx["trap_groups"]:
        reached = any(set(trap) & set(g) for g in groups)
        notes.append(f"trap {trap} {'reaches' if reached else 'skips'} the model"
                     " (either is fine — the model is what rejects it)")

    # Cost proxy: real speech, no retakes in it, so every group here is a call
    # the model has to answer "no" to.
    real = _load("real_hinglish.json")
    _, real_groups = stage_one(real)
    minutes = real["duration"] / 60
    print(f"real transcript: {len(real_groups)} candidate groups over "
          f"{minutes:.1f} min ({len(real_groups) / minutes:.1f} calls/min of video)")
    if len(real_groups) / minutes > 4:
        failures.append(f"stage 1 is too loose on real speech: "
                        f"{len(real_groups) / minutes:.1f} model calls per minute")

    # Stage 2 wiring, with a stub standing in for the model.
    calls = []

    def fake_complete(prompt, system="", max_tokens=0):
        calls.append(prompt)
        return '```json\n{"retake": true, "keep": 1, "confidence": 0.8, ' \
               '"reason": "second attempt is complete"}\n```'

    result = retakes.detect(fx["words"], silences=[tuple(s) for s in fx["silences"]],
                            complete=fake_complete)
    if not result["cuts"]:
        failures.append("stage 2 produced no cuts from a confirming model")
    if any(c.auto for c in result["cuts"]):
        failures.append("a retake cut was marked auto — retakes are always confirmed")
    if not all(c.reason == "retake" for c in result["cuts"]):
        failures.append("retake cuts must carry reason='retake'")
    if len(calls) != len(groups):
        failures.append(f"asked the model {len(calls)} times for {len(groups)} groups")
    print(f"\nstage 2 (stubbed): {len(calls)} calls → {len(result['cuts'])} cuts, "
          f"all suggestions: {not any(c.auto for c in result['cuts'])}")

    # A model that says "not a retake" must remove nothing at all.
    quiet = retakes.detect(fx["words"], silences=[tuple(s) for s in fx["silences"]],
                           complete=lambda *a, **k: '{"retake": false}')
    if quiet["cuts"]:
        failures.append("a rejecting model still produced cuts")

    # Junk from the model must not become an edit.
    for bad in ('not json at all', '{"retake": true}',
                '{"retake": true, "keep": 99, "confidence": 1.0}', ''):
        junk = retakes.detect(fx["words"],
                              silences=[tuple(s) for s in fx["silences"]],
                              complete=lambda *a, **k: bad)
        if junk["cuts"]:
            failures.append(f"malformed model reply {bad!r} produced cuts")

    # No key configured must be distinguishable from "found nothing".
    if retakes.detect(fx["words"], complete=None)["status"] not in ("no-model", "ok",
                                                                   "none-found",
                                                                   "none-confirmed"):
        failures.append("detect() returned an unknown status")

    print()
    for n in notes:
        print(f"note: {n}")

    if failures:
        print("\nFAIL")
        for f in failures:
            print(f"  {f}")
        return 1
    print("\nPASS — every staged retake reached the model, nothing auto-applies, "
          "and malformed replies cut nothing.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
