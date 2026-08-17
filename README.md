# Clippings

Turn long videos into viral short-form clips with animated Hinglish captions.

Two products live in this repo:

1. **Clipper** — paste a YouTube URL or upload a video, Gemini finds the 3-5 most viral moments, and each one is rendered as a vertical 9:16 clip with blur-pad background, face-aware cropping, and burned word-by-word captions.
2. **Caption engine** — a standalone transcribe → edit → render pipeline being productized for editors. Word-level Whisper transcription (Hindi/English code-switched speech supported), natural Hinglish romanization, fully customizable caption styles (fonts, colors, animations), and editor-friendly exports including transparent alpha overlays for Premiere/Final Cut/Resolve.

## Structure

```
backend/            FastAPI service
  main.py           API: auth, clip jobs, upload/URL processing
  clipper.py        download → Gemini viral-moment analysis → parallel clip rendering
  captions/         standalone caption engine (SaaS API + CLI)
    styles.py       CaptionStyle schema — the user-facing customization surface
    transcribe.py   Whisper word timestamps (mlx-whisper on Apple Silicon, CPU fallback)
    romanize.py     Devanagari → natural Hinglish (LLM pass with rule-based fallback)
    engine.py       word timings + style → animated ASS subtitles
    render.py       exports: burned MP4, alpha overlay .mov, .ass, .srt
    storage.py      durable jobs + Supabase Storage (signed uploads, retention cleanup)
    notify.py       completion notifications (in-app feed + Resend email)
    api.py          /captions/* routes
    cli.py          local end-to-end runs, no server needed
    schema.sql      caption_jobs table + captions bucket (run once in Supabase)
  supabase_client.py  auth + clip storage (signed URLs, TTL cleanup)
frontend/           Next.js app (Supabase auth, clip dashboard)
```

## Architecture decisions

Decisions made while productizing the caption engine, and why:

- **Web app first, NLE plugin later.** A Premiere/UXP plugin is a separate engineering track that reaches only Premiere users and iterates at marketplace-review speed. The web app reaches every editor, ships daily, and the plugin can come later as a thin wrapper over the same `/captions/*` API.
- **Alpha overlay export is the editor workflow.** A transparent QuickTime .mov (qtrle, alpha channel) drops onto any NLE timeline above the original footage. This delivers plugin-level integration without a plugin, and the source video never needs to be uploaded for it — the overlay renders from just width/height/duration/fps.
- **Uploads stay small by design.** Transcription needs audio, not video: the browser extracts ~3MB of audio client-side and posts that. Caption preview plays the local file in the browser. Only the burned-MP4 export needs the full video on the server.
- **Big files go direct to storage.** `/captions/upload-url` hands the browser a signed Supabase Storage URL; the video never passes through FastAPI (the legacy `/upload` route reads whole files into server memory — the captions flow doesn't). Resumable/chunked (TUS) uploads and background-upload-while-editing are the planned frontend patterns: the video uploads while the user fixes the transcript, so perceived upload time is near zero.
- **48-hour retention, then hard delete.** Sources and outputs expire `CAPTION_RETENTION_SECONDS` (default 48h) after job creation. Cleanup removes storage objects, local files, and job rows on the existing 30-minute cadence in `main.py`. Storage never accumulates.
- **Renders can take a while, so completion is push, not poll.** Two channels: an in-app feed (`GET /captions/notifications`, `seen` flag per job) and an email via Resend with a signed download link valid for the full retention window — the user doesn't need to keep the tab open or log back in.
- **Jobs are durable rows, not process memory.** `caption_jobs` (Postgres via Supabase) survives restarts. Jobs orphaned mid-render by a restart are reported as failed after `CAPTION_STALE_SECONDS` (default 30 min) instead of hanging forever.
- **Transcripts round-trip losslessly.** Words carry both scripts (`text` = original Devanagari, `hinglish` = romanized) plus per-word timings. Every re-style, correction, or re-export reuses the stored transcript — transcription runs once per video.
- **Romanization is LLM-first with a rules fallback.** Gemini rewrites words the way people actually type Hinglish ("chunautiyan", not IAST "cunatiyam"), batched 80 words per call with 1:1 alignment enforced. Without `GOOGLE_API_KEY`, a rule-based transliterator (indic-transliteration + schwa deletion + c→ch fixes) keeps the pipeline functional.
- **Transcription backend is auto-picked.** `mlx-whisper` (Apple Silicon GPU, large-v3-turbo) when available — a 4.5-min video transcribes in ~3.5 min vs 70+ min on CPU medium. Falls back to `openai-whisper` elsewhere. Production plan: faster-whisper on a small GPU box.
- **Styles are a validated JSON schema, not presets-only.** `CaptionStyle` (pydantic) exposes font, sizes, hex colors, words per line, position, and animation (`pop` / `fade` / `karaoke` / static highlight). The frontend style editor renders its controls from `GET /captions/style-schema`, so new knobs appear in the UI without frontend changes. Presets are just starting points users fork.
- **ffmpeg gotcha, documented in code:** the `ass` filter needs `:alpha=1` to write glyphs into the alpha channel; without it the overlay renders fully transparent.

## Caption engine

### Flow

1. **Transcribe** — Whisper with word timestamps.
2. **Romanize** — Devanagari → natural Hinglish, alignment preserved.
3. **Style** — a `CaptionStyle` JSON or a preset (`default`, `bold_impact`, `subtle`, `karaoke`).
4. **Export** — `burned` MP4, `overlay` alpha .mov, `ass`, or `srt`.

### CLI

```bash
cd backend
python -m captions.cli video.mp4                          # transcribe + burn, bold_impact style
python -m captions.cli video.mp4 --export overlay         # alpha overlay for editors
python -m captions.cli video.mp4 --style my_style.json    # custom style JSON
python -m captions.cli video.mp4 --transcript saved.json  # reuse a transcript, skip Whisper
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

```bash
# backend
cd backend
pip install fastapi uvicorn supabase yt-dlp google-generativeai moviepy \
            openai-whisper indic-transliteration pydantic python-multipart requests
pip install mlx-whisper   # Apple Silicon only, much faster transcription
uvicorn main:app --reload

# frontend
cd frontend
npm install && npm run dev
```

One-time: run `backend/captions/schema.sql` in the Supabase SQL editor (creates the `caption_jobs` table and `captions` bucket).

Requires `ffmpeg` on PATH.

Environment:

| Variable | Purpose |
|---|---|
| `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` | auth, storage, job rows |
| `GOOGLE_API_KEY` | Gemini (clip analysis + Hinglish romanization) |
| `CAPTION_RETENTION_SECONDS` | file/job lifetime, default 172800 (48h) |
| `RESEND_API_KEY`, `CAPTION_FROM_EMAIL` | completion emails (optional — skipped if unset) |
| `APP_URL` | link target in notification emails |
| `CAPTION_STALE_SECONDS` | orphaned-job timeout, default 1800 |
