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

## Timeline export (EDL / FCPXML)

### Editors get decisions, not a re-encode
`render_cut` flattens the edit into a new MP4. That is right for a finished
clip and wrong for anyone who still has grading, sound, or B-roll to do: it
throws away every cut point and costs a generation of quality. EDL and FCPXML
carry the same cuts as edit decisions, the NLE relinks the untouched original,
and every cut stays draggable.

Two formats because no single one is read everywhere. EDL (CMX3600) is plain
text and imports into Premiere, Resolve, Avid, and Final Cut, but carries
nothing except cuts. FCPXML is read by Final Cut and Resolve and keeps the clip
name, the media path, and markers.

### Record timecodes accumulate rounded source durations
An EDL's record times must be exactly contiguous. Converting each segment's
record time from floating-point seconds independently lets rounding error pile
up, and an EDL with non-contiguous record times imports with gaps or overlaps.
So each event's record-in is the previous event's record-out, and durations are
summed in whole frames.

### NTSC rates get drop-frame timecode
29.97 and 59.94 are labelled drop-frame; 23.976 is not, matching every NLE.
Labelling 29.97 material non-drop drifts roughly 3.6 seconds per hour against
the clock — invisible in review, and exactly the kind of thing a delivery gets
rejected for. `scripts/check_timeline.py` asserts the standard checkpoints,
including that hour one lands on 107892 frames.

The probed frame rate is snapped back to its exact rational before any
multiplication: ffprobe reads back rounded (29.97, not 30000/1001), and the
rounded value drifts about a tenth of a frame per hour for no reason.

### FCPXML times are exact rationals
Times are written as `1001/30000s`, never as decimals, because Final Cut
re-quantises anything else. Zero is written `0s`.

### The clip keeps the user's filename
The app works on its own copy of the upload, in a work directory swept after a
few days. The timeline points at that copy but is *named* for the user's
original, so when the path goes stale the NLE's relink matches on name and the
fix is one click instead of a hunt.

### Suggestions we did not apply become markers
A cut the user switched off is still something we noticed. In FCPXML it travels
as a marker on the clip that contains it, so the flag survives into the edit
instead of being lost at export. Markers landing inside a removed span are
dropped rather than emitted outside their clip, which Final Cut ignores
silently.

### Only the burned MP4 re-encodes
Subtitle and timeline exports need the re-timed words and nothing else. They
used to trigger a full `render_cut` first, spending minutes producing a video
file that was then thrown away.

## Retake detection (issue: lines delivered more than once)

### The model is the second stage, never the first
Handing a 40-minute transcript to a model and asking it to find the retakes is
expensive, unreproducible, and gets worse as the file grows. Instead, cheap
local arithmetic splits the transcript into phrases at real pauses and scores
each against the recent ones with lexical overlap. Only what survives that goes
to the model, as a handful of small, focused questions. On the project's
reference transcript this is about **one model call per minute of video**.

The two stages fail in opposite directions and are gated separately. Stage 1 is
scored on **recall** — anything it drops is gone for good, and a false candidate
costs one cheap call. Stage 2 is the precision stage.

### The model classifies, it never generates
It picks between spans stage 1 already computed. It proposes no timestamps and
rewrites no text, and a reply naming an out-of-range attempt is discarded. A
hallucination therefore cannot invent a cut inside a good sentence — the worst
it can do is decline to help.

### The model's reply is checked by exact type, not truthiness
JSON `"false"` is a truthy string and Python counts `True` as an `int`, so
`{"retake": "false"}` and `{"keep": true}` both survive a casual check and
become real cuts. The verdict must be the JSON boolean `true` and the index an
actual `int`. This is the difference between the guarantee holding and merely
being claimed, so `eval_retakes.py` asserts every one of those shapes cuts
nothing.

### A failed call is not a "no"
A rejected key, a quota wall, and the model saying "these are not retakes" are
three different outcomes. Collapsing them into `None` made an outage read to
the user as "your take is clean" — the worst possible failure for a feature
whose whole job is to find something. `llm.LLMError` carries a reason the user
can act on, detection stops at the first one (a rejected key does not start
working on the next call), and both callers say so.

### Caps are reported, never silent
Detection stops after 12 groups to bound cost on a long recording. A cap the
user cannot see reads as "we checked everything", so the number skipped is
returned and surfaced in both the CLI and the UI.

### Retakes never auto-apply
A filler cut is 300ms; a retake cut is eight seconds of speech, and a wrong one
destroys the take. Every retake arrives switched off, shown beside the take we
would keep, with both playable. In the CLI this is two flags: `--retakes`
reports, `--cut-retakes` applies.

### Phrases split on measured pauses, not word timings
The same trap as silence detection: Whisper stretches each word's end time to
the start of the next, so inter-word gaps read 0.00 across two seconds of
silence. Real pauses come from ffmpeg. Clause punctuation is the fallback when
there is no media, and it is genuinely worse — in the reference transcript the
first 151 words carry no punctuation at all, which ran them together into one
49-second "phrase" that then matched everything by containment.

"Measured, and there were none" is not the same as "not measured". An empty
silence list is an answer — the speaker never stopped, so there are no restarts
to find — and falling back to punctuation there invents boundaries against the
evidence. `None` means unmeasured; `[]` means measured and empty.

### Similarity uses overlap, with floors
A second attempt is routinely longer or shorter than the first, so Jaccard
punishes exactly the thing being detected; intersection over the *smaller* set
does not. That ratio flatters short phrases (two shared words out of five reads
as 0.40), so a match also needs at least 3 shared words, at least 1 shared
bigram, and takes within 2.5× of each other in length. Bigrams are what stop a
shared bag of Hinglish connectives from scoring on its own.

### No SDK for the cloud call
`requests` is already a dependency; `anthropic` and `google-generativeai` are
not, and adding one would put tens of megabytes into every platform build to
save a dozen lines. `captions/llm.py` posts to either API directly and picks
whichever key is set.

### The key is configured in the app, not the environment
Reading the key from `os.environ` alone made the feature unreachable for the way
Bolcap actually ships. A double-clicked app inherits launchd's environment on
macOS, not the shell's, and Windows behaves the same for a double-clicked exe —
so `export ANTHROPIC_API_KEY=...` in a terminal never arrived, and the retakes
card silently never appeared. Worst kind of failure: no error, nothing to
search for.

The key now lives in `~/.bolcap/config.json` (0600, in a 0700 directory) and is
pushed into the environment at startup, which keeps `captions/llm.py` reading
nothing but `os.environ` and keeps the engine layer unaware of the app's
config. A key already in the environment wins and cannot be overwritten from
the UI: CI, the CLI, and anyone running from a shell set it deliberately.

The key is never read back — the UI gets "configured", the provider, and the
last four characters.

It is written through a 0600 temporary file and swapped in with `os.replace`.
Writing first and chmod-ing after leaves a window where the default umask has
already put the key on disk as 0644, and a write that fails never reaches the
chmod at all.

"Never leaves the machine" was wrong and is not what the UI says any more: the
key is sent to the provider as the authentication header on every request.
What stays local is the audio, the video, and the key at rest.

An environment key also decides *which* provider is used. Injecting a saved
Anthropic key alongside an exported `GOOGLE_API_KEY` made `provider()` pick
Anthropic and silently ignore the choice the user had made, so the environment's
provider is recorded at startup and honoured explicitly.

### The key is the feature switch
Without one the retakes card still appears, but it explains what the feature
needs and takes the key inline instead of hiding. The CLI says so plainly.
Bolcap's promise is that nothing leaves the machine, so this one exception is
stated where the user will read it: transcript **text** is sent, never audio or
video.


## Fit to length ("get this under 60 seconds")

### Cheapest doubt per second, not shortest cut first
Auto cuts are free — safe by definition — so they are always in. Suggestions
are then added cheapest-first, where cost is `(1 - confidence) / duration`:
how much doubt each second of saving carries. That prefers one long confident
cut over three short shaky ones, which is what a person trimming by hand does.

### Length is recomputed, never summed
Cuts overlap. Adding up their durations claims the same second twice and stops
short of the target while reporting success, so the remaining length is
recomputed from the real kept spans after every addition.

### Over-cutting is a bug, not a rounding error
Greedy selection overshoots: the cut that finally crosses the line often makes
an earlier one unnecessary. A backward pass drops the shakiest cut the target
can do without, one at a time. Without it, a 52-second target on a 56-second
clip removed 8 seconds where 6 would do. `check_fit.py` asserts that every
applied suggestion is one the target could not reach without.

### A length target never applies a retake
A retake cut is seconds of real speech chosen by a cloud model, and it is the
one kind of cut that always waits for a person. Letting a number in a text box
apply one would undo that rule quietly. Retakes are excluded from selection and
the count left alone is reported.

### A target that cannot be met is said out loud
When every available cut still leaves the video over the target, the rest is
speech — and cutting speech to hit a number is the user's call, not the
software's. The shortfall is reported in both the CLI and the UI rather than
silently delivering something longer than asked for.

### The choice is made once, on the server
`tighten.fit_to_length` picks the cuts; the browser only switches on the
indices it returns. Reimplementing the choice in JavaScript is exactly how the
timeline and the export came to disagree about which words survived.


## Writes are same-origin only

The local server is loopback-only and unauthenticated, which is fine for
serving a page you opened yourself and not fine for writes. A multipart form
POST is CORS-safelisted, so any page you happen to visit can post to
`127.0.0.1` without being able to read the reply — enough to overwrite the
stored cloud key with the attacker's own, quietly sending every later
transcript to their account.

Every state-changing endpoint now refuses a request a browser has labelled
cross-site, by `Sec-Fetch-Site` or by an `Origin` that is not ours. A missing
`Origin` is allowed: that is a non-browser client, which already needs local
access to reach the port at all.
