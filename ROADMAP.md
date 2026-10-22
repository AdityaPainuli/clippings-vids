# Roadmap

Where the project stands, what's broken, and what comes next.
Last updated: 2026-10-22, against `main` at `6bceb0c`.

Settled decisions live in [DECISIONS.md](DECISIONS.md). This file is for the
open stuff. Every item has an issue. Claim it there before starting, see
[CONTRIBUTING.md](CONTRIBUTING.md).

## Where things stand

| Piece | State |
|---|---|
| Caption engine + CLI | Works end to end. All four CI checks and the unit tests pass. |
| Bolcap desktop app | `bolcap-v0.4.0` (2026-10-22) is current with `main`. Release builds are pinned and smoke-tested on all 4 platforms, first-run download included. |
| Hosted backend | Runs. `backend/captions/schema.sql` creates every Supabase table and bucket it uses. |
| Web frontend | Clipper only. No captions UI. Backend URL hardcoded to `localhost:8000`. Builds, but `npm run lint` fails. |

## Now: broken or risky

Things that stop a new user, or that put data at risk.

- #176 Upload filename path traversal writes outside `uploads/` (PR #110)
- #178 ffmpeg/ffprobe calls in render and tighten have no timeout
- #214 Caption retention deletes a source that a newer live job still uses
- #46 / #41 Uploads are read fully into memory and not validated (PR #110 for #46)

## Next: correctness

### Caption engine and CLI
- #179 `--export edl|fcpxml` silently burns an MP4 when there are no cuts
- #180 `--export ass` ignores `-o`
- #181 `romanize_words` overwrites existing `hinglish` values
- #182 LLM romanization never runs in the desktop app and bypasses `llm.py`
- #183 Retake phrase splitting needs ~0.69s pauses, not 0.45s
- #184 Four Tamil/Telugu filler entries can never match
- #185 SRT timestamps truncate milliseconds
- #43 (PR #155), #47, #54 (PR #55), #64, #76, #154, #117 (PR #120), #119 (PR #121), ASS "60.00" rounding (PR #141)
- Retakes cluster: #56, #60, #62, #66, #68, #70, #72, #78, #82

### Desktop app
- #186 Retakes and render share one job status
- #187 Transcribe doesn't check the model is installed
- #188 FCPXML points at a working copy the 3-day sweep deletes
- #189 Whisper model download isn't pinned to a revision or checksum
- #190 Model download progress is wrong
- #191 A failed `/api/setup/run` leaves the UI spinning forever
- #192 Browser opens onto whatever already holds port 8756
- #193 GET endpoints don't check Host (DNS rebinding)
- #194 Work dir ignores `BOLCAP_HOME`, cleanup only at startup
- #84 faster-whisper silently falls back to CPU

### Hosted backend and frontend
- #195 `DELETE /clips/{job_id}` leaves metadata rows and cached results
- #196 Concurrent jobs for the same video share one downloaded file
- #197 No global cap on clip render workers
- #198 Failed clip jobs leak files in `uploads/` and `output/`
- #199 Clipper returns a fake clip when `GOOGLE_API_KEY` is missing
- #200 `/status/{job_id}` returns the server-side video path
- #201 Refresh token is never stored, users are logged out after ~1h
- #202 `/my-clips` shows "undefined MB"
- #203 Backend URL hardcoded to `localhost:8000`
- #204 `npm run lint` fails
- #136, #102, #100, #96, #94, #111 (PR #123), #42

## Later: scale and product

- **Multi-worker hosted backend.** Jobs, SSE tokens, SSE events and the clip
  cache are all process-local: #74, #80, #86, #90. One shared store (the
  `caption_jobs` pattern) fixes all four.
- **Captions UI in the web app.** The paid tier in DECISIONS.md has an API but
  no frontend.
- **One full `requirements.txt`** for the hosted backend; move off the
  deprecated `google-generativeai` SDK.
- **Release smoke coverage.** Add tighten / fit / timeline
  export to `smoke_bolcap.sh`.
- **Health endpoints** #40 (PR #135). **Offline LLM** #36 (PR #51).
- **Pan-Indic** #32, #33, #34.
- Code signing for macOS/Windows builds (deferred, see BOLCAP.md).
