# Roadmap

Where the project stands, what's broken, and what comes next.
Last full audit: 2026-10-20, against `main` at `787a2b5`.

Settled decisions live in [DECISIONS.md](DECISIONS.md). This file is for the
open stuff. Items marked **(new)** were found in the audit and have no issue
yet. Open one before picking it up, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Where things stand

| Piece | State |
|---|---|
| Caption engine + CLI | Works end to end. All four CI checks and the unit tests pass. |
| Bolcap desktop app | Works from source. Last release `bolcap-v0.3.1` (2026-08-30) is 14 commits behind `main`, so no shipped build has the `neon` preset or the fixes since. |
| Hosted backend | Runs, but needs Supabase objects nothing in the repo creates, and has an auth bug that breaks DB writes after the first login (#170, below). |
| Web frontend | Clipper only. No captions UI. Backend URL hardcoded to `localhost:8000`. Builds, but `npm run lint` fails with 9 errors. |

## Now: broken or risky

Things that stop a new user, or that put data at risk.

- **Fresh installs couldn't transcribe (fixed in this audit).** PyAV 19 dropped
  the `metadata_errors` argument that faster-whisper 1.2.1 still passes, so
  every transcription died with
  `TypeError: open() got an unexpected keyword argument 'metadata_errors'`.
  `requirements-bolcap.txt` now pins `av<19`. The next release build would
  have shipped broken without it. The real fix is #108 (pin everything).
- **#170 Shared Supabase client gets swapped to a user session** (PR #171).
  `/auth/login`, `/auth/signup` and `/auth/refresh` call auth methods on the
  global service-role client. supabase-py v2 then sends that user's JWT on
  every later `supabase.table(...)` call. Under RLS that breaks `create_job`
  (caption transcribe/render 500) and makes `update_job` / cleanup silently
  match nothing. Fix: a throwaway client for auth calls.
- **(new) Missing Supabase setup.** The `clips` bucket and `clip_metadata` table
  are used but never created. Without them every clip job ends
  "All N clips failed". Add them to `schema.sql` and make it re-runnable.
- **#104** SSRF via user media URLs. **#46 / #41** unbounded, unvalidated uploads.
- **(new) Upload URLs never expire their files.** `/captions/upload-url` signs
  uploads to `{uid}/sources/...`, but retention only deletes paths referenced by
  a job row. Uploads that never get a job stay in the bucket forever.
- **#92** ffmpeg audio extraction can hang forever.

## Next: correctness

### Caption engine and CLI
- **(new)** `--export edl|fcpxml` silently burns an MP4 when there are no cuts
  (`cli.py:192`). It should write an empty timeline or error.
- **(new)** `--export ass -o out.ass` ignores `-o` (`cli.py:237`).
- **(new)** `romanize_words` overwrites an existing `hinglish` field, and the
  rule fallback writes Devanagari into it (`romanize.py:174`). Re-running loses
  user edits.
- **(new)** LLM romanization imports `google.generativeai`, which the desktop
  app doesn't ship, so the desktop app is always rule-based even with a Gemini
  key. It also skips `llm.py` (no Anthropic, no timeout). Route it through `llm.py`.
- **(new)** Retake phrase splitting applies `MIN_PAUSE=0.45` to silences already
  trimmed by tighten's padding, so a pause needs about 0.69s to count
  (`retakes.py:283`).
- **(new)** Four Tamil/Telugu filler entries can never match, because lang_detect
  never tags those words as ta/te (`tighten.py:68`). `therlusaa` looks like a typo.
- **(new)** SRT timestamps truncate instead of rounding (`render.py:72`).
- Existing: #43 (PR #155), #47, #54 (PR #55), #64, #76, #154, #117 (PR #120),
  #119 (PR #121), and ASS "60.00" rounding (PR #141).
- Retakes cluster: #56, #60, #62, #66, #68, #70, #72, #78, #82.

### Desktop app
- **(new)** Retakes and render share one `job["status"]`, so running both at once
  re-enables export early, or reports "none were actually retakes" falsely
  (`localapp/server.py`).
- **(new)** `/api/transcribe` doesn't check the model is installed, so it starts
  an untracked Hugging Face download. No lock on the model cache either.
- **(new)** FCPXML points at the working copy in `~/.bolcap/work`, which the
  3-day sweep deletes.
- **(new)** Whisper models aren't pinned to a revision or checksum (only ffmpeg is).
- **(new)** `/api/setup/run` failures leave the UI on "Downloading model…" forever.
- **(new)** Port already in use: the browser still opens onto whatever is on 8756.
- **(new)** `WORK_DIR` ignores `BOLCAP_HOME`. Cleanup only runs at startup.
- **(new)** GET endpoints don't check `Host`, so DNS rebinding can read setup
  status (home path, key hint). Writes are already Origin-checked.
- Existing: #84, #98 (docs fixed in this audit, close once reviewed).

### Hosted backend and frontend
- **(new)** `DELETE /clips/{job_id}` leaves `clip_metadata` rows and the clip
  cache entry, and 404s once the in-memory job is gone.
- **(new)** Concurrent jobs for the same YouTube video share `uploads/<id>.mp4`;
  the first to finish deletes it under the second.
- **(new)** No global cap on render work: each job spawns up to 4 ffmpeg+whisper
  processes.
- **(new)** Failed jobs leak files in `uploads/` and `output/`.
- **(new)** No `GOOGLE_API_KEY` means the clipper returns a placeholder 0-10s clip
  instead of failing.
- **(new)** `/status` returns the server-side `video_path`.
- **(new)** Frontend never stores the refresh token, so users are logged out
  after about an hour. `/my-clips` shows "undefined MB".
- Existing: #136, #102, #100, #96, #94, #111 (PR #123), #42.

## Later: scale and product

- **Multi-worker hosted backend.** Jobs, SSE tokens, SSE events and the clip
  cache are all process-local: #74, #80, #86, #90. One shared store (the
  `caption_jobs` pattern) fixes all four.
- **Captions UI in the web app.** The paid tier in DECISIONS.md has an API but
  no frontend.
- **Configurable frontend API URL** (`NEXT_PUBLIC_API_URL`) and a clean lint.
- **One full `requirements.txt`** for the hosted backend; move off the
  deprecated `google-generativeai` SDK.
- **Release v0.4.0** once `av<19` and #108 land. Add tighten / fit / timeline
  export to `smoke_bolcap.sh`.
- **Health endpoints** #40 (PR #135). **Offline LLM** #36 (PR #51).
- **Pan-Indic** #32, #33, #34.
- Code signing for macOS/Windows builds (deferred, see BOLCAP.md).

## Open PRs waiting on review

#171, #155, #141, #135, #123, #121, #120, #110, #55, #51. Several close
items above; reviewing them is the cheapest progress available.
