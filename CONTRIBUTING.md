# Contributing

Thanks for picking something up. This file covers the things that have actually
tripped people up on this repo, not generic open source advice.

## Before you write code

**Claim the issue first.** Comment on it and wait for a reply. Several people
have lost a weekend to a fix that was already open as a PR. Check the issue's
comments and the open PR list before you start.

**One issue per PR.** If you found three unrelated problems, that is three PRs.
A PR touching nine files across the backend and frontend will be closed and
asked to split, no matter how good the individual changes are.

**Don't reformat code you aren't changing.** That includes deleting comments.
The comments in this repo explain decisions (why RLS is enabled, why the filler
list excludes single vowels, why the LLM call uses `requests` instead of an
SDK). Stripping them makes a five line feature look like a two hundred line
rewrite and it will not get merged.

## Setup

```bash
cd backend
pip install -r requirements-bolcap.txt
uvicorn main:app --reload
```

You need `ffmpeg` and `ffprobe` on your PATH for anything that renders.

## Tests

This is where nearly every PR goes wrong, so read this part.

**Use stdlib `unittest`. Not pytest.** Tests live in `backend/tests/` and CI
runs them with:

```bash
python -m unittest discover -s tests -p "test_*.py"
```

Bare pytest functions contribute zero tests under that runner. They will pass
on your machine and run nothing in CI.

**Stub heavy imports.** `requirements-bolcap.txt` is deliberately small. It has
fastapi, uvicorn, python-multipart, pydantic, requests, faster-whisper and
indic-transliteration. That is the whole list, and it is the list CI installs.

These are **not** in that file, so they do not exist in CI even if you have
them locally: `yt_dlp`, `supabase`, `pytest`, `moviepy`, `whisper`, `cv2`,
`google-generativeai`, `anthropic`.

So importing `clipper` or `captions.storage` directly in a test is an
ImportError in CI even though it works locally. Stub what you don't need before
you import:

```python
import sys
from unittest import mock

for mod in ["yt_dlp", "google", "google.generativeai", "moviepy",
            "moviepy.editor", "dotenv", "whisper", "cv2"]:
    sys.modules.setdefault(mod, mock.MagicMock())

from clipper import _parse_vtt_to_text  # noqa: E402
```

**Don't add dependencies.** The desktop app bundles this list on every
platform, so a new package costs tens of megabytes per build. If you genuinely
need one, say why in the issue before you open the PR.

**Check your test actually tests your code.** Delete the line you are fixing
and re-run. If the test still passes, it was never testing anything. This has
happened here with a Pydantic validator whose test matched Pydantic's own
built-in error message.

## What CI runs

`.github/workflows/checks.yml`, on every push and PR:

| Step | What it guards |
|---|---|
| `scripts/eval_tighten.py` | Filler detection cuts no real words. 100% recall, zero false cuts |
| `scripts/check_timeline.py` | EDL and FCPXML timecode arithmetic |
| `scripts/eval_retakes.py` | Retakes reach the model, nothing auto-applies |
| `scripts/check_fit.py` | Fit to length never cuts more than the target needs |
| `unittest discover` | Everything in `backend/tests/` |

`eval_tighten.py` is the strictest one. If your change makes it cut a real
word, the build fails and the PR does not merge. Run it locally before you
push.

None of these need a model, ffmpeg, an API key, or network.

## Pull requests

Branch from `main`, name it for what it does (`fix/vtt-multiline-cues`).

Write a description that says what changed and why. If you reference the issue
with `Closes #123`, GitHub links them.

Say what you actually tested. "Existing test suite" when you never ran it is
worse than saying nothing.

## OSCI 2026

This repo is an OSCI'26 project. For a contribution to count, the PR must be
**merged** and carry the **`OSCI'26`** label. Maintainers add the label, you
don't need to. Open PRs and closed-unmerged PRs score nothing.

Issues carry a difficulty label (`easy`, `medium`, `hard`, `expert`) and a PR
inherits it when the body says `Closes #N`.

## On AI-assisted contributions

Use whatever tools you like. I do. What I care about is whether you understood
the change before you sent it.

The line is simple: **an AI can help you write the fix, it cannot be the only
thing that read the issue.** If you cannot explain in review why the bug
happens, the PR is not ready.

Things that get a PR closed without a detailed review:

- **Fixing a bug that does not exist.** Before you write the fix, reproduce the
  problem against current `main` and paste what you saw. Several PRs here have
  "fixed" behaviour that was already correct, including one that added a
  `math.isfinite` guard for a case the existing clamp already handled.
- **Filing an issue and the PR that fixes it minutes apart, repeatedly.** One or
  two is initiative. Twenty in an afternoon is not review-able and will be
  treated as one batch, not twenty contributions.
- **Tests that do not test the change.** Delete the line you are fixing. If the
  test still passes, it proves nothing. This has happened here with a validator
  whose test matched Pydantic's own built-in error message.
- **Claiming you ran things you did not run.** "All tests pass" on a suite that
  fails at import is the fastest way to lose the benefit of the doubt.
- **Generated PR descriptions that do not match the diff.** If the body
  documents functions that do not exist in the code, I stop trusting the rest
  of it.

None of this is about detecting AI. It is about whether a human checked the
work. A short PR where you clearly understood the problem beats a large
polished one you did not read, every time.

If a PR gets closed for this, it is not a ban. Open a focused one on an issue
you have actually reproduced and it gets reviewed like any other.
