#!/usr/bin/env python3
"""
Check CLI argument validation.

    python scripts/check_cli.py
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from captions.cli import parse_min_gap  # noqa: E402


def main():
    failures = []

    # Valid values should be accepted.
    for value in ("0.4", "1"):
        try:
            result = parse_min_gap(value)
            if result <= 0:
                failures.append(f"{value!r}: accepted non-positive value")
        except Exception as e:
            failures.append(f"{value!r}: unexpectedly rejected: {e}")

    # Invalid values should be rejected.
    for value in ("-1", "nan", "inf"):
        try:
            parse_min_gap(value)
            failures.append(f"{value!r}: unexpectedly accepted")
        except argparse.ArgumentTypeError:
            pass

    if failures:
        print("FAIL")
        for failure in failures:
            print(f"  {failure}")
        return 1

    print("PASS — min-gap validation is correct.")
    return 0


if __name__ == "__main__":
    sys.exit(main())