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

## Tighten (dead air, fillers, stutters)

### Never cut a word on spelling alone
The reference transcript settled this with data. It contains 7 filler-list
words — `kahin na kahin`, `pehla toh ye ki`, `yaani usne thoda context...`,
`haan aapne sahi samjha` — and **all 7 are genuine vocabulary**. A word-list
cutter scores 0% precision on real Hinglish. So words are classed, not matched:

- **non-lexical** (`umm`, `uh`, `hmm`) are not words in any language and are
  cut on sight;
- **lexical** (`matlab`, `toh`, `yaani`, `like`, `actually`) are real
  vocabulary that is *sometimes* filler, and list membership only makes an
  occurrence a candidate.

### Context decides, and pauses are the signal
Every genuine use in the reference transcript had **no pause on either side**.
So a lexical candidate is scored: pause before (+0.35), pause after (+0.35),
duration ≥1.6× that speaker's own median for that same token (+0.20), and ≥4
occurrences within 15s (+0.15). At ≥0.70 it is cut, 0.45–0.70 it is suggested
but not applied, below that it is not mentioned. The weights are deliberate:
pauses on both sides alone reach the threshold, and nothing else can get there
without them.

### Lexical fillers are opt-in
Off by default, in the CLI (`--aggressive-fillers`) and in the app (a
"filler-like real words" toggle). Whether `toh` is filler depends entirely on
the speaker and the use case, so it is the user's call, not a default. Out of
the box the tool cannot touch a real word.

### Precision is gated, recall is only reported
`scripts/eval_tighten.py` fails the run if any non-filler word is cut, and
prints recall without gating it. A wrongly cut word is a hole the user has to
hunt for; a missed filler is one click. Fixtures pair real speech (the
negatives) with staged stalling (the positives). The staged positives are the
weak half of that evidence — real loose takes would be worth more than any
tuning.

### Repeats are usually deliberate
All 8 repeated words in the reference transcript were intentional: 7
rhetorical restarts across a clause boundary (`sabse upar aaya Claude. Claude
ne...`) and one `alag-alag`, Hindi reduplication where saying it twice *is*
the word. Guards: clause punctuation on the first word, hyphenation, a known
reduplicative list, and a pause between the two.

### Silence comes from the audio, not from word timings
The spec assumed a gap between words means silence. It doesn't. Whisper emits
back-to-back word spans and stretches a word's end time across the pause after
it — on a clip with a real 2-second gap, every word-to-word gap was `0.00` and
one "word" lasted 2.44s. Silence is measured with ffmpeg `silencedetect`,
which is one cheap pass and is the only honest source. `silencedetect` failing
raises rather than reporting "no silence found".

### Cuts and captions re-time together
`apply_cuts` maps the transcript onto the cut timeline, so captions can never
drift from the edit. Words are assigned to kept spans by **largest overlap**,
not midpoint: Whisper's stretched spans put a real word's midpoint inside a
silence, and midpoint assignment silently deleted it from the captions.
Zero-length words fall back to containment for the same reason.

### Splices get a 15ms fade
Cutting audio at an arbitrary sample pops. Every kept segment fades in and out
over ~15ms — inaudible as a fade, and the difference between "sounds edited"
and "sounds broken". Frame-accurate cuts mean a real re-encode; stream copy can
only cut on keyframes.

### The preview skips, the export cuts
Browser playback jumps over removed spans so there is nothing to wait for. That
leaves a small audible seam the exported file will not have. The UI's
struck-through words use the same largest-overlap rule as the server, so what
looks removed and what is removed always agree.
