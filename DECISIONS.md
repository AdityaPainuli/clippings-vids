# Technical decisions

Decision log for Bolcap (the caption product) and the clipper. One entry per
decision: what we chose, why, and what was rejected. Newest last.

## Product

### Web app first, NLE plugin later
A Premiere CEP/UXP plugin is a separate engineering track (panel JS, marketplace
review, per-host testing) and reaches only Premiere users. The web app reaches
every editor and iterates daily. A plugin can come later as a thin wrapper over
the same `/captions/*` API.

### Alpha overlay export as the editor workflow
Transparent QuickTime .mov (qtrle + alpha) drops onto any NLE timeline above the
original footage. Plugin-level integration without plugin engineering, and the
source video never needs uploading for it — the overlay renders from just
width/height/duration/fps. This is the pro-tier export.

### Open-core distribution
The caption engine + a local desktop app (**Bolcap**) are the free community
product; the hosted web app is the paid convenience tier (GPU-fast
transcription, LLM romanization included, no setup). Local users cost nothing
and market the product; hosted users pay for what their own hardware can't do
well. Rejected: local-only paid licensing (honor-system revenue, heavy support
burden) and SaaS-only (no community growth channel).

### Pricing shape (hosted)
Credits per rendered video-minute, free tier with watermark on burned exports.
Marginal cost is ~$0.01-0.02/min vs ~$0.05-0.10/min market pricing. Don't rent
a GPU until managed-ASR bills exceed ~$75/mo (~1,800 audio-hours).

## Caption engine

### Styles are a validated JSON schema, not fixed presets
`CaptionStyle` (pydantic) exposes font, sizes, hex colors, words per line,
position, animation (`pop` / `fade` / `karaoke` / static). UIs render controls
from `GET /captions/style-schema`, so new knobs appear without frontend changes.
Users edit CSS-style hex; conversion to ASS `&HAABBGGRR` stays inside the
engine. Presets are forkable starting points.

### Transcripts round-trip losslessly
Words carry both scripts (`text` = original Devanagari, `hinglish` = romanized)
plus per-word timings. Every correction, re-style, or re-export reuses the
stored transcript — transcription runs once per video.

### Romanization: LLM-first, rules fallback
Gemini rewrites words the way people type Hinglish ("chunautiyan", not IAST
"cunatiyam"), batched 80 words/call with 1:1 alignment enforced; misaligned
responses fall back per-chunk. Without `GOOGLE_API_KEY`, a rule-based
transliterator (indic-transliteration + ṃ→n, c→ch, final schwa deletion) keeps
the pipeline functional offline. Rejected: rules-only (reads stilted — the LLM
pass is the quality moat).

### Transcription backend auto-selection
`mlx-whisper` (Apple Silicon GPU, large-v3-turbo) when importable, else CPU
whisper. Measured on a 4.5-min video: ~3.5 min on mlx GPU vs 70+ min on CPU
medium — the gap that drives every hosting and local-app decision below.
Planned (#4): `faster-whisper` (CTranslate2) replaces openai-whisper as the
portable backend — no PyTorch, small wheels, int8 CPU 2-4x faster, CUDA
auto-detect. Priority becomes mlx → faster-whisper → openai-whisper.

### ffmpeg gotchas (hard-won)
- The `ass` filter needs `:alpha=1` to write glyphs into the alpha channel;
  without it a transparent-canvas overlay renders fully invisible.
- Overlay canvas uses `format=yuva420p` lavfi color source, encoded qtrle.
- `probe_video` checks ffprobe exit code and stream presence — corrupt files
  fail with a readable error, not a JSON traceback.

## Hosted service

### Uploads stay small by design
Transcription needs audio, not video: the browser extracts ~3MB of audio
client-side. Caption preview plays the local file in the browser. Only the
burned-MP4 export needs the full video server-side.

### Big files go direct to storage
`/captions/upload-url` hands the browser a signed Supabase Storage URL; the
video never passes through FastAPI (the legacy clipper `/upload` buffers whole
files in server memory — the captions flow doesn't). Frontend plan: resumable
TUS chunks, uploading in the background while the user edits the transcript,
so perceived upload time is near zero. At scale, outputs move to Cloudflare R2
(zero egress fees); the signed-URL pattern doesn't change.

### 48-hour retention, then hard delete
Sources, outputs, and job rows expire `CAPTION_RETENTION_SECONDS` (default 48h)
after job creation; cleanup runs on the existing 30-minute cadence and removes
storage objects, local files, and rows. Storage never accumulates; the
completion email warns about the deadline.

### Completion is push, not poll
Renders take minutes; users shouldn't babysit a tab. Two channels: in-app feed
(`GET /captions/notifications` + `seen` flag) always on, and email via Resend
with a signed download link valid the full retention window (skipped silently
when `RESEND_API_KEY` is unset). Rejected: websockets (overkill for one event).

### Jobs are durable rows
`caption_jobs` in Postgres (Supabase) survives restarts. Jobs orphaned
mid-render are persisted as failed after `CAPTION_STALE_SECONDS` (default 30
min) on first read — FastAPI `BackgroundTasks` don't survive deploys. Planned:
a worker loop pulling `queued` rows replaces in-process execution so render
capacity scales separately from the API.

### Storage HTTP hygiene
All Supabase Storage REST calls carry explicit (connect, read) timeouts —
(10, 30) control-plane, (10, 600) transfers — so a network stall can't hang
request handlers or the cleanup cadence.

## Bolcap local app (issues #3-#9)

### Name: Bolcap
"bol" (speak) + captions. Lowercase `bolcap` in code, CLI, and release
artifacts; "Bolcap" in prose.

### Local web GUI over native UI
One codebase: thin localhost FastAPI server wrapping `captions/`, browser UI,
auto-opened. Reuses the whole Python engine as-is; UI components later seed the
SaaS editor. Rejected: Tauri/Rust + whisper.cpp (slickest binaries but means a
second engine to maintain) and Electron (200MB shell on top of Python anyway).

### Ship small, download heavy bits on first run
App download ~50-80MB. First launch fetches the whisper model (user-picked
size, honest speed labels, small default on CPU) and a static ffmpeg build for
the platform, checksummed, into the app data dir. Open fonts (Noto Sans
Devanagari + display faces) are bundled and passed via libass `fontsdir` —
Arial Black doesn't exist off macOS/Windows.

### Packaging: PyInstaller + Actions matrix
PyInstaller onedir, GitHub Actions matrix (windows-x64, macos-arm64, macos-x64,
linux-x64), artifacts attached to GitHub Releases on tag push, CI smoke test
renders a 5s fixture before publishing. Gatekeeper/SmartScreen stance is
decided before the first release (#9), not after the issue reports.
