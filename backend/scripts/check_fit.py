
#!/usr/bin/env python3
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from captions.tighten import Cut, fit_to_length, keep_segments

DURATION = 60.0


def sample():
    return [
        Cut(2, 4, "silence", 1.0, "gap", auto=True),
        Cut(10, 12, "silence", 1.0, "gap", auto=True),
        Cut(20, 26, "filler", 0.6, "long shaky"),
        Cut(30, 32, "filler", 0.95, "short confident"),
        Cut(40, 44, "repeat", 0.5, "medium"),
        Cut(48, 56, "retake", 0.8, "a whole take"),
        Cut(11, 13, "filler", 0.85, "overlaps a silence"),
    ]


def length_of(cuts):
    return sum(e - s for s, e in keep_segments(cuts, DURATION))


def main():
    failures = []
    cuts = sample()

    for target in (56, 54, 52, 50, 48, 44, 40, 20):
        r = fit_to_length(cuts, DURATION, target)

        if any(c.reason == "retake" for c in r["cuts"]):
            failures.append(f"target {target}: applied a retake")

        if r["protected"] != 1:
            failures.append(f"target {target}: retake not protected")

        for auto in (c for c in cuts if c.auto):
            if auto not in r["cuts"]:
                failures.append(f"target {target}: dropped auto cut")

        stated = round(length_of(r["cuts"]), 3)

        if abs(stated - r["duration"]) > 0.01:
            failures.append(f"target {target}: incorrect duration")

        reachable = r["duration"] <= target + 1e-6

        if r["reachable"] != reachable:
            failures.append(f"target {target}: incorrect reachability")

        if not reachable and r["shortfall"] <= 0:
            failures.append(f"target {target}: missing shortfall")

        for extra in r["added"]:
            without = [c for c in r["cuts"] if c is not extra]
            if length_of(without) <= target + 1e-6:
                failures.append(f"target {target}: unnecessary cut {extra.text}")

        print(f"target {target:>3}s -> {r['duration']:>6.2f}s "
              f"reachable={str(r['reachable']):<5} "
              f"added={[c.text for c in r['added']]}")

    overlapping = [
        Cut(10, 12, "silence", 1.0, "gap", auto=True),
        Cut(11, 13, "filler", 0.85, "overlaps it"),
    ]

    union = fit_to_length(overlapping, DURATION, 57)

    if abs(union["duration"] - 57) > 0.01:
        failures.append("overlapping cuts calculated incorrectly")

    if len(union["added"]) != 1:
        failures.append("overlapping suggestion not applied")

    summed = DURATION - sum(c.duration for c in union["cuts"])

    if abs(summed - union["duration"]) < 0.01:
        failures.append("overlap was calculated using sum instead of union")

    nested = [
        Cut(10, 20, "silence", 1.0, "big", auto=True),
        Cut(12, 14, "filler", 0.9, "inside it"),
    ]

    r = fit_to_length(nested, DURATION, 45)

    if r["added"]:
        failures.append("unnecessary nested cut applied")

    empty_ok = fit_to_length([], DURATION, DURATION + 10)

    if not empty_ok["reachable"] or empty_ok["duration"] != DURATION:
        failures.append("empty cuts incorrectly handled")

    empty_short = fit_to_length([], DURATION, 30)

    if empty_short["reachable"] or empty_short["shortfall"] != 30:
        failures.append("empty cuts shortfall incorrectly handled")

    for bad, name in (
        (float("inf"), "infinite"),
        (float("nan"), "NaN"),
        (0, "zero"),
        (-5, "negative")
    ):
        try:
            fit_to_length(cuts, DURATION, bad)
        except ValueError:
            pass
        else:
            failures.append(f"{name} target was accepted")

    only_retakes = [
        Cut(10, 30, "retake", 0.9, "take")
    ]

    r = fit_to_length(only_retakes, DURATION, 30)

    if r["cuts"] or r["reachable"]:
        failures.append("retake used automatically")

    if failures:
        print("\nFAIL")
        for failure in failures:
            print(" ", failure)
        return 1

    print("\nPASS - All video length fitting checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

