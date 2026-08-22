#!/usr/bin/env python3
"""
Check the rules behind fitting a video to a target length.

Choosing cuts to hit a length is the one place where the software decides how
much of someone's speech to remove, so the guarantees are asserted rather than
trusted:

  * a retake is never applied to satisfy a length target;
  * a target that cannot be reached is reported, never quietly missed;
  * the result never cuts more than the target needs.

    python scripts/check_fit.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from captions.tighten import Cut, fit_to_length, keep_segments  # noqa: E402

DURATION = 60.0


def sample():
    """
    Deliberately includes an auto cut and a suggestion that overlap.

    `_merge` keeps auto cuts and suggestions in separate passes, so overlap
    between the two is normal, not a corner case. With only disjoint spans an
    implementation that just sums cut durations passes every check here while
    over-counting the same second twice in real use.
    """
    return [
        Cut(2.0, 4.0, "silence", 1.00, "gap", auto=True),
        Cut(10.0, 12.0, "silence", 1.00, "gap", auto=True),
        Cut(20.0, 26.0, "filler", 0.60, "long shaky", auto=False),
        Cut(30.0, 32.0, "filler", 0.95, "short confident", auto=False),
        Cut(40.0, 44.0, "repeat", 0.50, "medium", auto=False),
        Cut(48.0, 56.0, "retake", 0.80, "a whole take", auto=False),
        # Overlaps the 10.0-12.0 silence above: together they remove 3s, not 4.
        Cut(11.0, 13.0, "filler", 0.85, "overlaps a silence", auto=False),
    ]


def length_of(cuts):
    return sum(e - s for s, e in keep_segments(cuts, DURATION))


def main():
    failures = []
    cuts = sample()

    for target in (56, 54, 52, 50, 48, 44, 40, 20):
        r = fit_to_length(cuts, DURATION, target)

        # A retake is seconds of real speech picked by a cloud model. Nothing
        # about a length target makes it safe to apply unasked.
        if any(c.reason == "retake" for c in r["cuts"]):
            failures.append(f"target {target}: applied a retake")
        if r["protected"] != 1:
            failures.append(f"target {target}: did not report the retake it left alone")

        # Auto cuts are safe by definition and cost nothing, so they are always in.
        for auto in (c for c in cuts if c.auto):
            if auto not in r["cuts"]:
                failures.append(f"target {target}: dropped a safe auto cut")

        stated = round(length_of(r["cuts"]), 3)
        if abs(stated - r["duration"]) > 0.01:
            failures.append(f"target {target}: reported {r['duration']}s, "
                            f"the cuts actually give {stated}s")

        reachable = r["duration"] <= target + 1e-6
        if r["reachable"] != reachable:
            failures.append(f"target {target}: reachable={r['reachable']} but "
                            f"result is {r['duration']}s")
        if not reachable and r["shortfall"] <= 0:
            failures.append(f"target {target}: missed the target but reported "
                            "no shortfall")

        # Over-cutting is removing content nobody asked to lose: every added
        # cut has to be one the target could not do without.
        for extra in r["added"]:
            without = [c for c in r["cuts"] if c is not extra]
            if length_of(without) <= target + 1e-6:
                failures.append(f"target {target}: {extra.text!r} was not needed")

        print(f"target {target:>3}s -> {r['duration']:>6.2f}s  "
              f"reachable={str(r['reachable']):<5} added="
              f"{[c.text for c in r['added']]}")

    # Overlap must be measured as a union. Summing durations would claim 4s
    # from the pair below when the timeline only loses 3s, so a target of 57
    # would look met while the video was still 57.x long.
    overlapping = [
        Cut(10.0, 12.0, "silence", 1.00, "gap", auto=True),
        Cut(11.0, 13.0, "filler", 0.85, "overlaps it", auto=False),
    ]
    union = fit_to_length(overlapping, DURATION, 57.0)
    if abs(union["duration"] - 57.0) > 0.01:
        failures.append(f"overlapping cuts measured as {union['duration']}s, "
                        "expected 57.0s from the union of 10.0-13.0")
    if len(union["added"]) != 1:
        failures.append("the overlapping suggestion was needed and not applied")
    summed = DURATION - sum(c.duration for c in union["cuts"])
    if abs(summed - union["duration"]) < 0.01:
        failures.append("the fixture no longer distinguishes union from sum")

    # A cut fully inside another buys nothing and must not be applied.
    nested = [
        Cut(10.0, 20.0, "silence", 1.00, "big", auto=True),
        Cut(12.0, 14.0, "filler", 0.90, "inside it", auto=False),
    ]
    r = fit_to_length(nested, DURATION, 45.0)
    if r["added"]:
        failures.append("applied a cut entirely contained by another")

    # An empty list is an answer, not bad input: it says the target is already
    # met, or by how much it is missed.
    empty_ok = fit_to_length([], DURATION, DURATION + 10)
    if not empty_ok["reachable"] or empty_ok["duration"] != DURATION:
        failures.append("an already-met target with no cuts was not reported")
    empty_short = fit_to_length([], DURATION, 30.0)
    if empty_short["reachable"] or empty_short["shortfall"] != 30.0:
        failures.append("no cuts and an impossible target reported no shortfall")

    # inf passes a bare `> 0` check, reaches JSON as a value it cannot hold,
    # and NaN gives a reachability answer that means nothing.
    for bad, why in ((float("inf"), "infinite"), (float("nan"), "NaN")):
        try:
            fit_to_length(cuts, DURATION, bad)
        except ValueError:
            pass
        else:
            failures.append(f"a {why} target was accepted")

    # With nothing but retakes on offer, a target changes nothing.
    only_retakes = [Cut(10.0, 30.0, "retake", 0.9, "take", auto=False)]
    r = fit_to_length(only_retakes, DURATION, 30)
    if r["cuts"] or r["reachable"]:
        failures.append("a retake was used to reach a target when it was the "
                        "only cut available")

    for bad, why in ((0, "zero"), (-5, "negative")):
        try:
            fit_to_length(cuts, DURATION, bad)
        except ValueError:
            pass
        else:
            failures.append(f"a {why} target was accepted")

    if failures:
        print("\nFAIL")
        for f in failures:
            print(f"  {f}")
        return 1
    print("\nPASS — retakes never auto-applied, missed targets reported, "
          "nothing cut that the target did not need.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
