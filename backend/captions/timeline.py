"""
Export cut decisions as an NLE timeline instead of a flattened video.

`render_cut` re-encodes: fine for a finished clip, useless to an editor who
still has grading and sound to do. An EDL or FCPXML carries the same cuts as
*edit decisions* — the NLE relinks them to the original file, so the media is
never touched and every cut stays draggable.

Two formats because no single one is read everywhere:

  * **EDL (CMX3600)** — ancient, plain text, imported by Premiere, Resolve,
    Avid, and Final Cut. Carries cuts and nothing else.
  * **FCPXML** — Final Cut Pro and Resolve. Keeps the clip name, embeds the
    media path, and can carry markers for cuts we suggested but did not make.

Frame accuracy is the whole job here. Two rules keep it honest:

  * Record timecodes accumulate rounded *source* durations. Rounding each
    segment's record time independently lets error pile up, and an EDL whose
    record times are not contiguous imports with gaps or overlaps.
  * NTSC rates (29.97, 59.94) get drop-frame timecode. Labelling 29.97fps
    material as non-drop drifts about 3.6 seconds per hour against the clock,
    which is exactly the kind of error nobody notices until the delivery is
    rejected.
"""

import os
from urllib.parse import quote


def timebase(fps: float) -> tuple:
    """
    (frames per timecode second, drop-frame?) for a real frame rate.

    29.97 counts in a base of 30 and drops labels to stay near the clock;
    23.976 counts in a base of 24 and does not (there is no standard
    drop-frame form for 24, so it is left non-drop as every NLE expects).
    """
    base = int(round(fps))
    if base <= 0:
        raise ValueError(f"Bad frame rate: {fps}")
    ntsc = abs(fps - base * 1000.0 / 1001.0) < 0.01
    return base, (ntsc and base in (30, 60))


def exact_fps(fps: float) -> float:
    """
    Snap a probed rate to its exact rational.

    ffprobe is read back rounded (29.97, not 30000/1001). Multiplying seconds
    by the rounded value drifts about a tenth of a frame per hour — harmless
    on a short clip, wrong on a long one, and free to avoid.
    """
    base, _ = timebase(fps)
    ntsc = abs(fps - base * 1000.0 / 1001.0) < 0.01
    return base * 1000.0 / 1001.0 if ntsc else float(base)


def _drop_adjust(frame: int, base: int) -> int:
    """
    Convert a frame index into the frame *number the drop-frame label counts*.

    Drop-frame skips two labels (four at 60) at the top of every minute except
    every tenth minute. Nothing is dropped from the media, only from the
    numbering.
    """
    drop = 2 if base == 30 else 4
    per_min = base * 60 - drop
    per_10min = base * 600 - drop * 9
    tens, rem = divmod(frame, per_10min)
    extra = drop * ((rem - drop) // per_min) if rem >= drop else 0
    return frame + drop * 9 * tens + extra


def timecode(frame: int, fps: float) -> str:
    """Frame index → SMPTE timecode. Drop-frame uses ';' before the frames."""
    base, drop = timebase(fps)
    f = _drop_adjust(frame, base) if drop else frame
    h, rem = divmod(f, base * 3600)
    m, rem = divmod(rem, base * 60)
    s, fr = divmod(rem, base)
    sep = ";" if drop else ":"
    return f"{h:02d}:{m:02d}:{s:02d}{sep}{fr:02d}"


def _events(keep: list, fps: float) -> list:
    """
    Keep spans → frame-accurate events with contiguous record times.

    Each event is (src_in, src_out, rec_in, rec_out) in frames. Out points are
    exclusive, matching CMX3600.
    """
    rate = exact_fps(fps)
    events, rec = [], 0
    for s, e in keep:
        src_in = int(round(s * rate))
        src_out = int(round(e * rate))
        if src_out <= src_in:
            continue                    # shorter than a frame — nothing to cut to
        length = src_out - src_in
        events.append((src_in, src_out, rec, rec + length))
        rec += length
    return events


# ── EDL ──────────────────────────────────────────────────────────────────────

def _reel(name: str) -> str:
    """CMX3600 reels are 8 characters of uppercase alphanumerics."""
    clean = "".join(c for c in os.path.basename(name).upper()
                    if c.isalnum())[:8]
    return (clean or "BOLCAP").ljust(8)


def build_edl(keep: list, fps: float, source_name: str,
              title: str = "BOLCAP TIGHTEN") -> str:
    events = _events(keep, fps)
    if not events:
        raise ValueError("Nothing to export — every span was cut")

    _, drop = timebase(fps)
    reel = _reel(source_name)
    lines = [f"TITLE: {title}",
             f"FCM: {'DROP FRAME' if drop else 'NON-DROP FRAME'}", ""]

    for i, (si, so, ri, ro) in enumerate(events, start=1):
        # AA/V = both audio channels plus video, C = cut (no transition).
        lines.append(
            f"{i:03d}  {reel} AA/V  C        "
            f"{timecode(si, fps)} {timecode(so, fps)} "
            f"{timecode(ri, fps)} {timecode(ro, fps)}"
        )
        lines.append(f"* FROM CLIP NAME: {os.path.basename(source_name)}")
        lines.append("")

    return "\n".join(lines)


def export_edl(keep: list, fps: float, source_name: str, out_path: str,
               title: str = "BOLCAP TIGHTEN") -> str:
    with open(out_path, "w", encoding="utf-8", newline="\r\n") as f:
        f.write(build_edl(keep, fps, source_name, title))
    return out_path


# ── FCPXML ───────────────────────────────────────────────────────────────────

_AUDIO_RATES = {
    32000: "32k", 44100: "44.1k", 48000: "48k",
    88200: "88.2k", 96000: "96k", 176400: "176.4k", 192000: "192k",
}


def _rational(frames: int, fps: float) -> str:
    """
    FCPXML times are exact rationals, never decimals.

    29.97fps is 30000/1001, so a time must be expressed in ticks of 1001 over
    30000 — writing "2.002s" would be silently re-quantised by Final Cut.
    """
    if not frames:
        return "0s"
    base, _ = timebase(fps)
    ntsc = abs(fps - base * 1000.0 / 1001.0) < 0.01
    return f"{frames * 1001}/{base * 1000}s" if ntsc else f"{frames}/{base}s"


def _frame_duration(fps: float) -> str:
    base, _ = timebase(fps)
    ntsc = abs(fps - base * 1000.0 / 1001.0) < 0.01
    return f"1001/{base * 1000}s" if ntsc else f"1/{base}s"


def _xml_escape(text: str) -> str:
    return (text.replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;").replace('"', "&quot;"))


def _file_url(path: str) -> str:
    r"""
    Absolute path → file:// URI.

    Windows needs care: percent-encoding a native path turns `C:\Users\x` into
    `C%3A%5CUsers%5Cx`, which no NLE can resolve. Separators become forward
    slashes, the drive-letter colon is left alone, and the result gets the
    third slash that makes `file:///C:/Users/x` a valid URI.
    """
    p = os.path.abspath(path).replace("\\", "/")
    drive, rest = os.path.splitdrive(p)
    if drive:                                  # "C:" — keep the colon literal
        return "file:///" + drive + quote(rest)
    return "file://" + quote(p)


def build_fcpxml(keep: list, fps: float, width: int, height: int,
                 video_path: str, duration: float,
                 name: str = "Bolcap Tighten",
                 markers: list | None = None,
                 clip_name: str | None = None,
                 audio: dict | None = None) -> str:
    """
    A single-track sequence of the kept spans, relinked to the original media.

    `clip_name` is what the editor sees and what their NLE matches on when the
    path goes stale. It is separate from `video_path` because the app works on
    its own copy of the upload: pointing the timeline at a working file whose
    *name* is the user's original means a relink is one click, not a hunt.

    `markers` is an optional list of Cut-like objects (start, reason, text) for
    spans we flagged but did not remove, dropped onto the timeline where the
    editor will actually look for them.

    `audio` is `render.probe_audio` output. Channel counts are declared only
    when they have actually been measured: an NLE trusts them on relink, so
    asserting stereo over a mono or multichannel file mismaps the audio. When
    it is unknown the attributes are left out, which FCPXML allows.
    """
    events = _events(keep, fps)
    if not events:
        raise ValueError("Nothing to export — every span was cut")

    _, drop = timebase(fps)
    tc_format = "DF" if drop else "NDF"
    total_src = int(round(duration * exact_fps(fps)))
    total_rec = events[-1][3]
    clip = _xml_escape(clip_name or os.path.basename(video_path))

    has_audio = True if audio is None else bool(audio.get("has_audio"))
    channels = (audio or {}).get("channels")
    audio_attrs = ""
    if has_audio and channels:
        audio_attrs = f' audioSources="1" audioChannels="{int(channels)}"'
    seq_audio = ""
    if has_audio and channels in (1, 2):
        seq_audio = f' audioLayout="{"mono" if channels == 1 else "stereo"}"'
    # audioRate is an enumerated token in FCPXML, not a number in hertz.
    # Anything outside the list is left off rather than guessed at.
    rate = _AUDIO_RATES.get((audio or {}).get("sample_rate"))
    if has_audio and rate:
        seq_audio += f' audioRate="{rate}"' 

    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<!DOCTYPE fcpxml>",
        '<fcpxml version="1.10">',
        "  <resources>",
        f'    <format id="r1" name="BolcapFormat" frameDuration="{_frame_duration(fps)}"'
        f' width="{width}" height="{height}"/>',
        f'    <asset id="r2" name="{clip}" start="0s"'
        f' duration="{_rational(total_src, fps)}" format="r1"'
        f' hasVideo="1" hasAudio="{1 if has_audio else 0}"{audio_attrs}>',
        f'      <media-rep kind="original-media" src="{_xml_escape(_file_url(video_path))}"/>',
        "    </asset>",
        "  </resources>",
        "  <library>",
        '    <event name="Bolcap">',
        f'      <project name="{_xml_escape(name)}">',
        f'        <sequence format="r1" duration="{_rational(total_rec, fps)}"'
        f' tcStart="0s" tcFormat="{tc_format}"{seq_audio}>',
        "          <spine>",
    ]

    # Markers hang off the clip that contains them, positioned relative to
    # that clip's own source start.
    pending = list(markers or [])
    for si, so, ri, ro in events:
        inner = []
        for m in pending:
            mf = int(round(getattr(m, "start", 0.0) * exact_fps(fps)))
            if si <= mf < so:
                label = getattr(m, "text", "") or getattr(m, "reason", "cut")
                inner.append(
                    f'              <marker start="{_rational(mf, fps)}"'
                    f' duration="{_frame_duration(fps)}"'
                    f' value="{_xml_escape(str(label))}"/>'
                )
        out.append(
            f'            <asset-clip ref="r2" name="{clip}"'
            f' offset="{_rational(ri, fps)}" start="{_rational(si, fps)}"'
            f' duration="{_rational(so - si, fps)}" format="r1"'
            f' tcFormat="{tc_format}"' + (">" if inner else "/>")
        )
        if inner:
            out.extend(inner)
            out.append("            </asset-clip>")

    out += ["          </spine>", "        </sequence>", "      </project>",
            "    </event>", "  </library>", "</fcpxml>", ""]
    return "\n".join(out)


def export_fcpxml(keep: list, fps: float, width: int, height: int,
                  video_path: str, duration: float, out_path: str,
                  name: str = "Bolcap Tighten", markers: list | None = None,
                  clip_name: str | None = None, audio: dict | None = None) -> str:
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(build_fcpxml(keep, fps, width, height, video_path,
                             duration, name, markers, clip_name, audio))
    return out_path
