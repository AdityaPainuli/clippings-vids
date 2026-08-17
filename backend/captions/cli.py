"""
CLI — run the full caption flow locally, no server needed.

  python -m captions.cli video.mp4                          # transcribe + burn, default style
  python -m captions.cli video.mp4 --preset bold_impact
  python -m captions.cli video.mp4 --style my_style.json --export overlay
  python -m captions.cli video.mp4 --transcript saved.json  # reuse transcript, skip whisper
  python -m captions.cli video.mp4 --script devanagari      # keep original script
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
                    choices=["burned", "overlay", "ass", "srt"])
    ap.add_argument("--transcript", help="reuse a saved transcript JSON")
    ap.add_argument("--script", default="hinglish", choices=["hinglish", "devanagari"])
    ap.add_argument("--language", default=None, help="force language code (e.g. hi)")
    ap.add_argument("-o", "--output", help="output path")
    args = ap.parse_args()

    from . import engine, render, romanize, styles, transcribe

    base = os.path.splitext(args.video)[0]

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

    if args.style:
        with open(args.style, encoding="utf-8") as f:
            style = styles.CaptionStyle(**json.load(f))
    else:
        style = styles.STYLE_PRESETS[args.preset]

    info = render.probe_video(args.video)
    text_key = "hinglish" if args.script == "hinglish" else "text"

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
        out = render.burn_video(args.video, ass_path,
                                args.output or f"{base}_subtitled.mp4")
    print(f"done: {out}")


if __name__ == "__main__":
    sys.exit(main())
