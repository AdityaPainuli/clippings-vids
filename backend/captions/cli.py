"""
CLI — run the full caption flow locally, no server needed.

  python -m captions.cli video.mp4                          # transcribe + burn, default style
  python -m captions.cli video.mp4 --preset bold_impact
  python -m captions.cli video.mp4 --style my_style.json --export overlay
  python -m captions.cli video.mp4 --transcript saved.json  # reuse transcript, skip whisper
  python -m captions.cli video.mp4 --script devanagari      # keep original script
  python -m captions.cli video.mp4 --tighten-report         # what would be cut?
  python -m captions.cli video.mp4 --tighten                # cut dead air + fillers
"""

import argparse
import json
import os
import sys


def main():
    ap = argparse.ArgumentParser(description="Hinglish caption engine")
    ap.add_argument("video")
    ap.add_argument("--preset", default="bold_impact", help="built-in style preset")
    ap.add_argument("--style", help="path to a CaptionStyle JSON (overrides --preset)")
    ap.add_argument("--export", default="burned",
                    choices=["burned", "overlay", "ass", "srt", "edl", "fcpxml"],
                    help="edl/fcpxml emit the cuts as an NLE timeline "
                         "(no re-encode); they require --tighten")
    ap.add_argument("--transcript", help="reuse a saved transcript JSON")
    ap.add_argument("--script", default="hinglish", choices=["hinglish", "devanagari"])
    ap.add_argument("--language", default=None, help="force language code (e.g. hi)")
    ap.add_argument("-o", "--output", help="output path")

    tg = ap.add_argument_group("tighten — remove dead air, fillers, stutters")
    tg.add_argument("--tighten", action="store_true",
                    help="apply auto-confidence cuts before captioning")
    tg.add_argument("--tighten-report", action="store_true",
                    help="report what would be cut, change nothing")
    tg.add_argument("--min-gap", type=float, default=0.40,
                    help="silence longer than this is cut (seconds)")
    tg.add_argument("--no-fillers", action="store_true",
                    help="leave filler words alone")
    tg.add_argument("--aggressive-fillers", action="store_true",
                    help="also cut context-scored real words like 'toh', 'na'")
    args = ap.parse_args()

    from . import engine, render, romanize, styles, tighten, timeline, transcribe

    base = os.path.splitext(args.video)[0]

    if args.style:
        with open(args.style, encoding="utf-8") as f:
            style = styles.CaptionStyle(**json.load(f))
    else:
        style = styles.STYLE_PRESETS[args.preset]
    text_key = "hinglish" if args.script == "hinglish" else "text"

    if args.export in ("edl", "fcpxml") and not args.tighten:
        print("--export edl/fcpxml describes what to cut; add --tighten")
        return 2

    if args.transcript:
        with open(args.transcript, encoding="utf-8") as f:
            transcript = json.load(f)
    else:
        print("Transcribing...")
        transcript = transcribe.transcribe_video(args.video, language=args.language)
        print(f"  {len(transcript['words'])} words ({transcript['backend']})")
        if args.script == "hinglish":
            print("Romanizing to Hinglish...")
            transcript["words"] = romanize.romanize_words(transcript["words"])
        transcript_path = f"{base}_transcript.json"
        with open(transcript_path, "w", encoding="utf-8") as f:
            json.dump(transcript, f, ensure_ascii=False, indent=1)
        print(f"  transcript saved: {transcript_path}")

    # ── Tighten ───────────────────────────────────────────────────────────
    info = render.probe_video(args.video)
    cut_video = None

    if args.tighten or args.tighten_report:
        cfg = tighten.TightenConfig(
            min_gap=args.min_gap,
            fillers=not args.no_fillers,
            lexical_fillers=args.aggressive_fillers,
        )
        cuts = tighten.detect(transcript["words"], cfg, duration=info["duration"],
                              media_path=args.video)
        s = tighten.summarize(cuts, info["duration"])
        print(f"Tighten: {s['original_duration']}s → {s['tightened_duration']}s "
              f"(−{s['removed_seconds']}s, {s['removed_percent']}%)")
        for reason, d in sorted(s["by_reason"].items()):
            print(f"  {reason}: {d['count']} cuts, {d['seconds']}s")
        if s["suggested_cuts"]:
            print(f"  ({s['suggested_cuts']} lower-confidence cuts not applied)")

        if args.tighten_report:
            for c in cuts[:40]:
                mark = "cut " if c.auto else "sugg"
                print(f"  {mark} {c.start:7.2f}–{c.end:7.2f} {c.reason:<8} "
                      f"{c.confidence:.2f}  {c.text}")
            if len(cuts) > 40:
                print(f"  ... {len(cuts) - 40} more")
            return

        applied = [c for c in cuts if c.auto]
        result = tighten.apply_cuts(transcript["words"], applied, info["duration"])
        transcript["words"] = result["words"]      # captions re-timed to the cut

        if args.export in ("edl", "fcpxml"):
            # The point of a timeline export is that the media is never
            # touched — the NLE relinks the original and applies these cuts.
            name = os.path.basename(args.video)
            if args.export == "edl":
                out = timeline.export_edl(result["kept"], info["fps"], name,
                                          args.output or f"{base}.edl")
            else:
                out = timeline.export_fcpxml(
                    result["kept"], info["fps"], info["width"], info["height"],
                    args.video, info["duration"],
                    args.output or f"{base}.fcpxml",
                    name=os.path.splitext(name)[0],
                    audio=render.probe_audio(args.video),
                    # Cuts we flagged but did not make become markers, so the
                    # editor can find them instead of rewatching for them.
                    markers=[c for c in cuts if not c.auto])
            # These captions are timed to the cut timeline and mean nothing
            # against the uncut source, so they ship with it.
            srt = render.export_srt(transcript["words"], f"{base}_tightened.srt",
                                    style.words_per_line, text_key=text_key)
            print(f"done: {out}")
            print(f"  captions for that timeline: {srt}")
            return

        # Only a burned MP4 needs the media physically cut. `ass` and `srt`
        # need nothing but the re-timed words, and `overlay` draws on a blank
        # canvas — re-encoding for any of them produces a video file that is
        # then thrown away.
        if args.export == "burned":
            print("Cutting video...")
            cut_video = f"{base}_tightened.mp4"
            render.render_cut(args.video, result["kept"], cut_video)
            info = render.probe_video(cut_video)
            print(f"  {cut_video}")
        else:
            info = {**info, "duration": result["duration"]}

    source_video = cut_video or args.video

    ass_path = f"{base}.ass"
    with open(ass_path, "w", encoding="utf-8") as f:
        f.write(engine.build_ass(transcript["words"], style,
                                 info["width"], info["height"], text_key=text_key))

    if args.export == "ass":
        print(f"done: {ass_path}")
        return
    if args.export == "srt":
        out = render.export_srt(transcript["words"], args.output or f"{base}.srt",
                                style.words_per_line, text_key=text_key)
    elif args.export == "overlay":
        print("Rendering alpha overlay...")
        out = render.render_overlay(ass_path, args.output or f"{base}_overlay.mov",
                                    info["width"], info["height"],
                                    info["duration"], info["fps"])
    else:
        print("Burning captions...")
        out = render.burn_video(source_video, ass_path,
                                args.output or f"{base}_subtitled.mp4")
    print(f"done: {out}")


if __name__ == "__main__":
    sys.exit(main())
