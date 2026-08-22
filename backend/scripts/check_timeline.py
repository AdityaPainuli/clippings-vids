#!/usr/bin/env python3
"""
Check the timecode arithmetic behind EDL/FCPXML export.

An off-by-one frame here is invisible in review and obvious in an NLE, and
drop-frame is the classic place to get it wrong, so the known checkpoints are
asserted rather than eyeballed.

    python scripts/check_timeline.py
"""

import os
import sys
import xml.dom.minidom

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from captions import timeline  # noqa: E402

NTSC30 = 30000 / 1001
NTSC60 = 60000 / 1001

# Frame index → expected label. The one-hour entry is the standard proof that
# drop-frame is implemented and not merely labelled: at 29.97 exactly 107892
# frames elapse in an hour of clock time.
DROP_FRAME_CASES = [
    (NTSC30, 0, "00:00:00;00"),
    (NTSC30, 1799, "00:00:59;29"),
    (NTSC30, 1800, "00:01:00;02"),      # ;00 and ;01 are skipped
    (NTSC30, 17981, "00:09:59;29"),
    (NTSC30, 17982, "00:10:00;00"),     # tenth minute drops nothing
    (NTSC30, 107892, "01:00:00;00"),
    (NTSC60, 3599, "00:00:59;59"),
    (NTSC60, 3600, "00:01:00;04"),      # four labels skipped at 59.94
]

NON_DROP_CASES = [
    (25.0, 25, "00:00:01:00"),
    (30.0, 30, "00:00:01:00"),
    (24000 / 1001, 24, "00:00:01:00"),  # 23.976 has no drop-frame form
    (25.0, 90000, "01:00:00:00"),
]


def _seconds(rational: str) -> float:
    """FCPXML time string ("1001/30000s" or "0s") back to seconds."""
    body = rational.rstrip("s")
    if "/" in body:
        num, den = body.split("/")
        return int(num) / int(den)
    return float(body or 0)


def main():
    failures = []

    for fps, frame, want in DROP_FRAME_CASES + NON_DROP_CASES:
        got = timeline.timecode(frame, fps)
        if got != want:
            failures.append(f"timecode({frame}, {fps:.3f}) = {got}, want {want}")

    if timeline.timebase(NTSC30) != (30, True):
        failures.append("29.97 should count in base 30 with drop-frame")
    if timeline.timebase(24000 / 1001) != (24, False):
        failures.append("23.976 should count in base 24 without drop-frame")
    if timeline.timebase(30.0) != (30, False):
        failures.append("true 30fps is not drop-frame")

    # Record timecodes must butt up against each other exactly. Rounding each
    # segment's record time on its own lets error accumulate, and an EDL with
    # non-contiguous record times imports with gaps or overlaps.
    keep = [(0.0, 6.05), (8.0, 12.3), (14.9, 20.0), (20.0, 20.01)]
    for fps in (NTSC30, 25.0, NTSC60):
        events = timeline._events(keep, fps)
        for a, b in zip(events, events[1:]):
            if a[3] != b[2]:
                failures.append(f"record times not contiguous at {fps:.3f}: {a} → {b}")
        total = sum(o - i for i, o, _, _ in events)
        if events and events[-1][3] != total:
            failures.append(f"record length {events[-1][3]} != source total {total}")

    # A span shorter than one frame cannot be cut to, so it is dropped rather
    # than emitted as a zero-length event no NLE will accept.
    if any(o - i <= 0 for i, o, _, _ in timeline._events(keep, NTSC30)):
        failures.append("emitted a zero-length event")

    edl = timeline.build_edl(keep, NTSC30, "/tmp/My Clip.mov")
    if "FCM: DROP FRAME" not in edl:
        failures.append("29.97 EDL must declare DROP FRAME")
    reel = edl.splitlines()[3].split()[1]
    if len(reel) > 8 or not reel.isalnum():
        failures.append(f"reel name {reel!r} is not 8 alphanumeric characters")

    if "NON-DROP FRAME" not in timeline.build_edl(keep, 25.0, "clip.mov"):
        failures.append("25fps EDL must declare NON-DROP FRAME")

    doc = timeline.build_fcpxml(keep, NTSC30, 1080, 1920, "/tmp/a & b.mp4", 20.0)
    try:
        xml.dom.minidom.parseString(doc)
    except Exception as e:                      # noqa: BLE001
        failures.append(f"FCPXML is not well-formed: {e}")
    if "1001/30000s" not in doc:
        failures.append("NTSC FCPXML must use exact rational frame durations")
    if "&amp;" not in doc:
        failures.append("filenames must be XML-escaped")

    # Markers are positioned in the asset's own timeline, so each one must land
    # inside the clip that carries it — a marker outside its clip is silently
    # dropped by Final Cut, which looks like the feature never worked.
    class _M:
        def __init__(self, start, text):
            self.start, self.text = start, text

    marked = timeline.build_fcpxml(
        keep, NTSC30, 1080, 1920, "/tmp/a.mp4", 20.0,
        markers=[_M(3.2, "matlab"), _M(16.0, "ye ye"), _M(7.0, "inside a cut")])
    dom = xml.dom.minidom.parseString(marked)
    placed = []
    for clip in dom.getElementsByTagName("asset-clip"):
        c_start = _seconds(clip.getAttribute("start"))
        c_dur = _seconds(clip.getAttribute("duration"))
        for m in clip.getElementsByTagName("marker"):
            at = _seconds(m.getAttribute("start"))
            placed.append(m.getAttribute("value"))
            if not (c_start <= at < c_start + c_dur):
                failures.append(f"marker {m.getAttribute('value')!r} at {at:.2f}s "
                                f"falls outside its clip [{c_start:.2f}, "
                                f"{c_start + c_dur:.2f})")
    if sorted(placed) != ["matlab", "ye ye"]:
        failures.append(f"expected the two markers inside kept spans, got {placed}")

    if failures:
        print("FAIL")
        for f in failures:
            print(f"  {f}")
        return 1
    print(f"PASS — {len(DROP_FRAME_CASES + NON_DROP_CASES)} timecode checkpoints, "
          "contiguous record times, well-formed FCPXML.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
