<img src="backend/localapp/icons/bolcap.png" width="88" align="right" alt="Bolcap icon">

# BolCAP

Turn long videos into viral short-form clips with animated Hinglish captions.

<img src="docs/demo-captions.gif" width="230" align="right" alt="Word-by-word Hinglish captions animating on a vertical video">

Two products live in this repo:

1. **Clipper** — paste a YouTube URL or upload a video, Gemini finds the 3-5 most viral moments, and each one is rendered as a vertical 9:16 clip with blur-pad background, face-aware cropping, and burned word-by-word captions.
2. **Bolcap** — a transcribe → edit → render caption pipeline for editors ("bol" = speak, + captions). Word-level Whisper transcription (Hindi/English code-switched speech supported), natural Hinglish romanization, fully customizable caption styles (fonts, colors, animations), and editor-friendly exports including transparent alpha overlays for Premiere/Final Cut/Resolve. Ships two ways: a hosted web app, and a **free cross-platform desktop app — see [BOLCAP.md](BOLCAP.md) to download and install it**.

The GIF is real output: word-by-word Hinglish captions with the spoken word
highlighted, burned by the engine in this repo.

All technical choices and their rationale live in [DECISIONS.md](DECISIONS.md).
Known bugs, gaps, and what comes next live in [ROADMAP.md](ROADMAP.md).

<br clear="right">

## Structure

```
backend/            FastAPI service
  main.py           API: auth, clip jobs, upload/URL processing
  clipper.py        download → Gemini viral-moment analysis → parallel clip rendering
  captions/         Bolcap caption engine (SaaS API + CLI)
    styles.py       CaptionStyle schema — the user-facing customization surface
    transcribe.py   Whisper word timestamps (mlx-whisper → faster-whisper → openai-whisper)
    romanize.py     Devanagari → natural Hinglish (LLM pass with rule-based fallback)
    engine.py       word timings + style → animated ASS subtitles
    render.py       exports: burned MP4, alpha overlay .mov, .ass, .srt
    tighten.py      finds dead air, filler words, and stutters worth cutting
    retakes.py      finds lines delivered more than once, picks the keeper
    timeline.py     cuts as an EDL / FCPXML timeline (no re-encode)
    llm.py          one cloud call over plain HTTP, no SDK (retakes only)
    storage.py      durable jobs + Supabase Storage (signed uploads, retention cleanup)
    notify.py       completion notifications (in-app feed + Resend email)
    api.py          /captions/* routes
    cli.py          local end-to-end runs, no server needed
    schema.sql      Supabase tables + buckets (caption_jobs, clip_metadata, captions, clips)
  supabase_client.py  auth + clip storage (signed URLs, TTL cleanup)
frontend/           Next.js app for the clipper (login via backend /auth/*, clip dashboard)
```

## Bolcap caption engine

### The desktop app

Drop in a video, and everything happens on your machine. Whisper transcribes it,
the words are romanized into natural Hinglish, and you get a transcript you can
correct word by word plus a live caption preview over your own footage.

![The Bolcap window: video with live caption preview, editable transcript, and style controls](docs/ui.jpg)

Click any word to jump the video there, double-click to retype it, then style
the captions and export — burned into an MP4, or as a transparent overlay `.mov`
for your editing timeline. [Download and install it →](BOLCAP.md)

Needs macOS 14, Windows 10, or glibc 2.38 (Ubuntu 24.04) and newer — the
[install notes](BOLCAP.md#install) say why, and the app checks before you get
as far as picking a video.

### Tighten

Bolcap can find the parts of a take worth cutting: dead air, filler words, and
stutters. Cuts show up on a timeline you can argue with — click a greyed span to
bring it back, and playback skips whatever is still removed.

Filler detection does not work off a word list. In a real 757-word transcript,
every occurrence of "toh", "na", "haan", and "yaani" was doing grammatical work,
so a list-matching cutter would have deleted seven real words and scored 0%
precision. Instead, sounds with no lexical meaning ("umm", "uh", "hmm") are cut
on sight, and real words that are *sometimes* fillers are scored on context and
left alone by default. The reasoning is in [DECISIONS.md](DECISIONS.md), and
`scripts/eval_tighten.py` runs in CI on every push, failing the build if any
real word gets cut.

**Fit to length** takes a target — "1:00" — and applies the least doubtful cuts
needed to reach it, dropping any it turns out not to need. If the target is out
of reach it says by how much rather than quietly handing back something longer.
It never applies a retake to hit a number.

Cuts leave two ways. A flattened MP4 with the captions burned on, or an **EDL /
FCPXML timeline** that carries the cuts into Premiere, Resolve, or Final Cut and
relinks your original file, so nothing is re-encoded and every cut stays
draggable. In FCPXML, cuts you turned down travel along as markers; EDL has no
way to carry them.

### Retakes

The other big time sink is a line delivered three times until it comes out
right. Bolcap groups the attempts, picks the keeper, and shows them side by
side so you can play each one before cutting anything.

Cheap local arithmetic does the finding: split the transcript at real pauses,
score each phrase against the recent ones. Only what survives goes to a cloud
model, about one call per minute of video, and the model only ever *chooses*
between spans that were already measured — it proposes no timestamps and
rewrites no text, so a bad answer cannot invent a cut inside a good sentence.

This is the one feature that leaves your machine, and only with a key set.
Paste one into the Retakes panel in the app, or set it in the environment for
CLI runs:

```bash
export ANTHROPIC_API_KEY=...      # or GOOGLE_API_KEY
```

A key pasted in the app is stored in `~/.bolcap/config.json`, readable by your
user account alone. It is sent to the provider you pick, as the authentication
header on each request — nowhere else, and never to any Bolcap infrastructure
(there isn't any). An environment variable wins over a saved key, and the app
will not overwrite one.

Your **transcript text** is sent. Audio and video never are. Retakes never apply
themselves — a wrong retake cut removes seconds of real speech, so every one
waits for you.

### Flow

1. **Transcribe** — Whisper with word timestamps.
2. **Romanize** — Devanagari → natural Hinglish, alignment preserved.
3. **Tighten** (optional) — find dead air, fillers, and stutters, and cut them.
   Retake detection lives here too, behind an API key.
4. **Style** — a `CaptionStyle` JSON or a preset (`default`, `bold_impact`, `subtle`, `karaoke`, `neon`).
5. **Export** — `burned` MP4, `overlay` alpha .mov, `ass`, `srt`, or an `edl` / `fcpxml` timeline.

### CLI

```bash
cd backend
python -m captions.cli video.mp4                          # transcribe + burn, bold_impact style
python -m captions.cli video.mp4 --export overlay         # alpha overlay for editors
python -m captions.cli video.mp4 --style my_style.json    # custom style JSON
python -m captions.cli video.mp4 --transcript saved.json  # reuse a transcript, skip Whisper

python -m captions.cli video.mp4 --tighten-report          # what would be cut, and why
python -m captions.cli video.mp4 --tighten                 # cut it, captions re-timed to match
python -m captions.cli video.mp4 --tighten --export fcpxml # cuts to your NLE, no re-encode
python -m captions.cli video.mp4 --tighten --fit 1:00      # trim to a target length
python -m captions.cli video.mp4 --tighten --aggressive-fillers  # also cut context-scored fillers
python -m captions.cli video.mp4 --retakes                 # report repeated lines
python -m captions.cli video.mp4 --tighten --cut-retakes   # and remove them
```

Transcripts are saved next to the video (`*_transcript.json`) so re-styling never re-transcribes.

### API

```
POST /captions/upload-url          signed direct-to-storage upload URL for big videos
POST /captions/transcribe          audio/video (file or storage_path) → transcript job
GET  /captions/presets             built-in style presets
GET  /captions/style-schema        JSON schema for building a style-editor UI
POST /captions/render              transcript + style + export → render job (email on finish)
GET  /captions/jobs                my jobs
GET  /captions/jobs/{id}           job status + transcript
GET  /captions/download/{id}       redirect to signed download URL
GET  /captions/notifications       unseen finished jobs (bell badge)
POST /captions/notifications/seen  mark feed items read
```

All `/captions/*` routes require a Supabase bearer token (same auth as the clipper).

## Clipper

`POST /process-url` or `POST /upload` with optional `instructions`, `clip_style` (funny / educational / emotional / controversial / highlights) and `caption_style`. Jobs run in the background; poll `GET /status/{job_id}`. Finished clips upload to Supabase Storage with signed URLs and a 6-hour TTL.

## Setup

There are two ways to run this. Most people want the first one.

### Bolcap only (desktop app or CLI, no accounts, no cloud)

Prebuilt zips: see [BOLCAP.md](BOLCAP.md#install). From source, with Python 3.12
and `ffmpeg`/`ffprobe` on PATH:

```bash
cd backend
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements-bolcap.txt -c constraints-bolcap.txt
python -m localapp                       # opens http://127.0.0.1:8756
python -m captions.cli video.mp4         # or skip the UI
```

The first run downloads a Whisper model (`medium` by default, ~1.5 GB). Set
`BOLCAP_MODEL=small` for a quicker first try on English footage. On Apple
Silicon, `pip install mlx-whisper` makes transcription much faster.

### Hosted stack (clipper + `/captions/*` API + Next.js frontend)

Needs a Supabase project, a Gemini key, Python 3.12, Node 20.9+, and `ffmpeg`.
There is no full requirements file yet; `requirements-bolcap.txt` alone is not
enough to import `main.py`.

```bash
# backend (run from backend/, it writes uploads/ output/ captions_output/ here)
cd backend
pip install -r requirements-bolcap.txt
pip install supabase python-dotenv yt-dlp google-generativeai "moviepy>=2" opencv-python
pip install openai-whisper  # optional legacy fallback, pulls PyTorch
uvicorn main:app --reload   # http://localhost:8000

# frontend
cd frontend
npm install && npm run dev  # http://localhost:3000
```

The frontend has the backend URL hardcoded as `http://localhost:8000`, so a
deployment anywhere else needs a code change. It only covers the clipper;
there is no captions UI in the web app yet.

Supabase, one time:

1. Run `backend/captions/schema.sql` in the SQL editor. It creates the
   `caption_jobs` and `clip_metadata` tables and the private `captions` and
   `clips` buckets. It is safe to re-run.
2. Enable the email/password auth provider.

Environment (backend reads `backend/.env`):

| Variable | Purpose |
|---|---|
| `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` | **required**, the app will not import without them |
| `GOOGLE_API_KEY` | Gemini clip analysis and LLM romanization. Without it the clipper returns a placeholder 0-10s clip |
| `CLIP_TTL_SECONDS` | clip lifetime and signed URL expiry, default 21600 (6h) |
| `CLEANUP_INTERVAL`, `JOB_TTL_SECONDS` | cleanup loop period (1800) and in-memory job lifetime (7200) |
| `CAPTION_RETENTION_SECONDS` | caption file/job lifetime, default 172800 (48h) |
| `CLIP_RENDER_WORKERS` | Maximum worker processes per clip-render pool; default 4, minimum 1. A process-local semaphore allows only one render pool at a time per backend process. |
| `CAPTION_STALE_SECONDS` | orphaned caption job timeout, default 1800 |
| `RESEND_API_KEY`, `CAPTION_FROM_EMAIL`, `APP_URL` | completion emails (optional, skipped if unset) |
| `CAPTION_FONT`, `CAPTION_FONTSIZE`, `CAPTION_COLOR`, `CAPTION_HIGHLIGHT`, `CAPTION_WORDS`, `CAPTION_MARGIN_V`, ... | clipper caption defaults (see `clipper.py`) |
| `CAPTIONS_CPU_MODEL`, `CAPTIONS_MLX_MODEL`, `CAPTIONS_FONTS_DIR` | Whisper model names and font dir for the caption engine |
| `ANTHROPIC_API_KEY`, `BOLCAP_ANTHROPIC_MODEL`, `BOLCAP_GEMINI_MODEL` | retake detection model (CLI and desktop app) |

Known gaps in the hosted stack are tracked in [ROADMAP.md](ROADMAP.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).

ffmpeg is not redistributed here. The desktop app downloads official
prebuilt binaries into the user's own machine on first run (or uses the
ones already on their PATH) and calls them as separate programs, so their
GPL terms cover those binaries rather than this source. NOTICE has the
details.
