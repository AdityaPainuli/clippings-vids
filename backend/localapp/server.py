"""
Bolcap local server — localhost-only API around the captions engine.

Jobs live in memory (single user, single process). The uploaded source
video stays on disk per job so transcribe → style → export never
re-uploads. Work dir defaults to ~/.bolcap/work and is wiped per job on
completion of the final export download.
"""

import json
import os
import threading
import uuid
from pathlib import Path
from typing import Dict, Literal, Optional

from fastapi import (BackgroundTasks, Depends, FastAPI, File, Form, HTTPException,
                     Request, UploadFile)
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import ValidationError

from captions import (engine, llm, render, retakes, romanize, styles, tighten,
                      timeline, transcribe)
from . import assets

# Loopback only — there is no auth. LAN exposure requires the explicit
# opt-in AND is still the user's own risk (documented, not recommended).
HOST = os.getenv("BOLCAP_HOST", "127.0.0.1")
if HOST not in ("127.0.0.1", "localhost", "::1") and os.getenv("BOLCAP_ALLOW_LAN") != "1":
    HOST = "127.0.0.1"
PORT = int(os.getenv("BOLCAP_PORT", "8756"))
WORK_DIR = Path(os.getenv("BOLCAP_WORK_DIR", Path.home() / ".bolcap" / "work"))
WORK_DIR.mkdir(parents=True, exist_ok=True)

# Sweep work files older than this on startup — re-exports stay possible
# within the window, disk doesn't grow forever (single-user local app).
WORK_TTL_DAYS = float(os.getenv("BOLCAP_WORK_TTL_DAYS", "3"))


def _sweep_stale_work():
    import time
    cutoff = time.time() - WORK_TTL_DAYS * 86400
    removed = 0
    for f in WORK_DIR.iterdir():
        try:
            if f.is_file() and f.stat().st_mtime < cutoff:
                f.unlink()
                removed += 1
        except OSError:
            pass
    if removed:
        print(f"[bolcap] removed {removed} work file(s) older than {WORK_TTL_DAYS:g} days")


_sweep_stale_work()

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Bolcap")
assets.apply_environment()

jobs: Dict[str, dict] = {}
setup_progress: Dict[str, object] = {"status": "idle"}


def _allowed_origins() -> set:
    return {f"http://{h}:{PORT}" for h in (HOST, "127.0.0.1", "localhost")}


def same_origin(request: Request):
    """
    Refuse cross-site writes.

    The server is loopback-only and unauthenticated, which is fine for reading
    a page you opened yourself and not fine for writes: a multipart form POST
    is CORS-safelisted, so any web page you happen to visit can post to
    127.0.0.1 without needing to read the reply. That is enough to overwrite
    the stored cloud key with the attacker's, sending every later transcript
    to their account.

    A browser always labels such a request — `Sec-Fetch-Site: cross-site`, or
    an `Origin` that is not ours. A missing Origin means a non-browser client
    (the smoke test, curl), which already needs local access to reach here.
    """
    if request.headers.get("sec-fetch-site") in ("cross-site", "same-site"):
        raise HTTPException(status_code=403, detail="Cross-origin request refused")
    origin = request.headers.get("origin")
    if origin is not None and origin not in _allowed_origins():
        raise HTTPException(status_code=403, detail="Cross-origin request refused")


WRITE = [Depends(same_origin)]


def _retakes_payload(job: dict) -> dict | None:
    r = job.get("retakes")
    if not r:
        return None
    return {**r, "cuts": [c.to_dict() for c in r["cuts"]]}


def _job(job_id: str) -> dict:
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


# ── First-run setup ──────────────────────────────────────────────────────────

@app.get("/api/setup/status")
async def setup_status():
    # `llm` tells the UI whether to offer retake detection at all. Everything
    # else in Bolcap runs offline; this is the one feature that needs a key,
    # so it is surfaced rather than failing when clicked.
    return {**assets.setup_status(), "progress": setup_progress,
            "llm": llm.provider(), "api_keys": assets.api_key_status()}


@app.post("/api/setup/run", dependencies=WRITE)
async def setup_run(model: str = Form(assets.DEFAULT_MODEL)):
    if setup_progress.get("status") == "running":
        return {"status": "running"}
    setup_progress.clear()
    setup_progress.update({"status": "running", "step": "starting",
                           "total_bytes": 0, "done_bytes": 0})

    def _run():
        assets.run_setup(model, setup_progress)   # persists the model choice
        if setup_progress.get("status") == "ready":
            assets.apply_environment()            # PATH + active model + fonts

    threading.Thread(target=_run, daemon=True).start()
    return {"status": "running"}


# ── Transcribe ───────────────────────────────────────────────────────────────

def _transcribe_worker(job_id: str, video_path: str, language: Optional[str],
                       hinglish: bool):
    job = jobs[job_id]
    try:
        job["status"] = "transcribing"
        result = transcribe.transcribe_video(video_path, language=language)
        if hinglish:
            job["status"] = "romanizing"
            result["words"] = romanize.romanize_words(result["words"])
        job["transcript"] = result
        job["video_info"] = render.probe_video(video_path)
        job["status"] = "ready"
    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)[:500]


@app.post("/api/transcribe", dependencies=WRITE)
async def transcribe_endpoint(
    file: UploadFile = File(...),
    language: Optional[str] = Form(None),
    hinglish: bool = Form(True),
):
    if not assets.ffmpeg_ready():
        raise HTTPException(status_code=409,
                            detail="ffmpeg/ffprobe not set up yet — run first-run setup")
    job_id = uuid.uuid4().hex[:12]
    video_path = WORK_DIR / f"{job_id}_{os.path.basename(file.filename or 'video.mp4')}"
    with open(video_path, "wb") as f:
        while chunk := await file.read(1 << 20):
            f.write(chunk)

    jobs[job_id] = {"status": "queued", "video_path": str(video_path),
                    "filename": file.filename, "outputs": {}}
    threading.Thread(target=_transcribe_worker,
                     args=(job_id, str(video_path), language, hinglish),
                     daemon=True).start()
    return {"job_id": job_id}


# ── Tighten ──────────────────────────────────────────────────────────────────

@app.post("/api/tighten", dependencies=WRITE)
async def tighten_endpoint(
    job_id: str = Form(...),
    silence: bool = Form(True),
    fillers: bool = Form(True),
    lexical_fillers: bool = Form(False),   # opt-in: these are real words
    repeats: bool = Form(True),
    min_gap: float = Form(0.40),
):
    """
    Propose cuts. Nothing is applied — the UI decides what to keep.

    lexical_fillers defaults off because words like "toh" and "yaani" are
    usually doing grammatical work; see DECISIONS.md.
    """
    job = _job(job_id)
    if not job.get("transcript"):
        raise HTTPException(status_code=409, detail="Transcribe first")

    cfg = tighten.TightenConfig(
        silence=silence, fillers=fillers, lexical_fillers=lexical_fillers,
        repeats=repeats, min_gap=max(0.15, min(2.0, min_gap)),
    )
    duration = job["video_info"]["duration"]
    cuts = tighten.detect(job["transcript"]["words"], cfg, duration=duration,
                          media_path=job["video_path"])
    return {
        "cuts": [c.to_dict() for c in cuts],
        "summary": tighten.summarize(cuts, duration),
        "duration": duration,
    }


@app.post("/api/fit", dependencies=WRITE)
async def fit_endpoint(job_id: str = Form(...), target: float = Form(...),
                       cuts: str = Form(...)):
    """
    Choose which of the proposed cuts to apply to reach a target length.

    Takes the cut list the UI is holding rather than re-detecting, so the
    selection is made against exactly what the user is looking at — and so
    there is one implementation of the choice, in tighten.fit_to_length,
    instead of one here and another in the browser.

    Returns the indices to switch on. Retakes are never among them.
    """
    job = _job(job_id)
    if not job.get("video_info"):
        raise HTTPException(status_code=409, detail="Transcribe first")

    try:
        raw = json.loads(cuts)
        # An analysed clip can legitimately have nothing to cut. Rejecting that
        # stopped Fit from doing the one useful thing left: saying whether the
        # target is already met, and by how much it is missed if not.
        if not isinstance(raw, list):
            raise ValueError("cuts must be a JSON array")
        parsed = [tighten.Cut(float(c["start"]), float(c["end"]),
                              c.get("reason", "silence"),
                              float(c.get("confidence", 1.0)),
                              str(c.get("text", ""))[:200],
                              auto=bool(c.get("auto")))
                  for c in raw]
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Bad cuts: {e}")

    duration = job["video_info"]["duration"]
    try:
        result = tighten.fit_to_length(parsed, duration, target)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    picked = {id(c) for c in result["cuts"]}
    return {
        "selected": [i for i, c in enumerate(parsed) if id(c) in picked],
        "duration": result["duration"],
        "target": result["target"],
        "reachable": result["reachable"],
        "shortfall": result["shortfall"],
        "protected": result["protected"],
        "original_duration": round(duration, 3),
    }


@app.post("/api/settings/api-key", dependencies=WRITE)
async def set_api_key(provider: str = Form(...), key: str = Form("")):
    """
    Save (or clear) the cloud model key.

    The key is never read back out — the response says which provider is
    configured and shows the last four characters, nothing more.
    """
    if provider not in assets.KEY_ENV:
        raise HTTPException(status_code=400, detail=f"Unknown provider: {provider}")
    try:
        assets.save_api_key(provider, key)
    except PermissionError as e:
        # Set in the launch environment: that is a deliberate choice made
        # outside the app, and the app must not quietly replace it.
        raise HTTPException(status_code=409, detail=str(e))
    except (OSError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Could not save key: {e}")
    return {"api_keys": assets.api_key_status(), "llm": llm.provider()}


# ── Retakes ──────────────────────────────────────────────────────────────────

def _retake_worker(job_id: str, words: list):
    job = jobs[job_id]
    try:
        job["status"] = "finding-retakes"
        job.pop("error", None)
        job["retakes"] = retakes.detect(words, media_path=job["video_path"])
        job["status"] = "ready"
    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)[:500]


@app.post("/api/retakes", dependencies=WRITE)
async def retakes_endpoint(job_id: str = Form(...),
                           transcript: Optional[str] = Form(None)):
    """
    Start retake detection. Unlike everything else in Bolcap this sends text
    to a cloud model, so it only runs when the user asks for it and only if a
    key is configured.

    The edited transcript is accepted for the same reason /api/render takes it:
    the user's corrections are what they can see on screen. Detecting against
    the raw Whisper output would send stale text to the provider and return
    attempts whose wording disagrees with the transcript in front of them.
    """
    job = _job(job_id)
    if not job.get("transcript"):
        raise HTTPException(status_code=409, detail="Transcribe first")
    if not llm.available():
        raise HTTPException(
            status_code=409,
            detail="Retake detection needs a cloud model. Set ANTHROPIC_API_KEY "
                   "or GOOGLE_API_KEY and restart Bolcap.")

    words = job["transcript"]["words"]
    if transcript:
        try:
            data = json.loads(transcript)
            if not isinstance(data, dict) or not isinstance(data.get("words"), list):
                raise ValueError("transcript must be an object with a words array")
            if not data["words"]:
                raise ValueError("empty transcript")
            words = data["words"]
        except (json.JSONDecodeError, ValueError) as e:
            raise HTTPException(status_code=400, detail=f"Bad transcript: {e}")

    threading.Thread(target=_retake_worker, args=(job_id, words),
                     daemon=True).start()
    return {"job_id": job_id, "provider": llm.provider()}


# ── Styles ───────────────────────────────────────────────────────────────────

@app.get("/api/presets")
async def presets():
    return {"presets": {name: s.model_dump() for name, s in styles.STYLE_PRESETS.items()}}


@app.get("/api/style-schema")
async def style_schema():
    return styles.CaptionStyle.model_json_schema()


# ── Render ───────────────────────────────────────────────────────────────────

ExportFormat = Literal["burned", "overlay", "ass", "srt", "edl", "fcpxml"]


def _render_worker(job_id: str, export: str, style: styles.CaptionStyle,
                   text_key: str, words: list, cuts: list | None = None,
                   markers: list | None = None):
    job = jobs[job_id]
    try:
        job["status"] = "rendering"
        job.pop("error", None)      # a retry must not report the last failure
        info = job["video_info"]
        base = WORK_DIR / job_id
        stem = os.path.splitext(job.get("filename") or "video")[0]
        source = job["video_path"]

        kept = None
        if cuts:
            # Cut first, then re-time the words onto the new timeline so the
            # captions and the cuts can never disagree.
            cut_objs = [tighten.Cut(c["start"], c["end"], c.get("reason", "silence"),
                                    c.get("confidence", 1.0), auto=True) for c in cuts]
            result = tighten.apply_cuts(words, cut_objs, info["duration"])
            if not result["kept"]:
                raise RuntimeError("Every part of the video was cut")
            words = result["words"]
            kept = result["kept"]
            info = {**info, "duration": result["duration"]}
            # Only the burned MP4 needs the media physically cut. Subtitle and
            # timeline exports need nothing but the re-timed words, and
            # re-encoding for them costs minutes to produce a file nobody opens.
            if export == "burned":
                source = str(base) + "_cut.mp4"
                render.render_cut(job["video_path"], kept, source)

        if export in ("edl", "fcpxml"):
            if not kept:
                raise RuntimeError("A timeline export describes cuts — run Tighten first")
            # Deliberately the *original* media's geometry: the NLE relinks to
            # the untouched file and applies these cuts itself.
            src_info = job["video_info"]
            marker_objs = [tighten.Cut(m["start"], m.get("end", m["start"]),
                                       m.get("reason", "cut"), 0.0,
                                       text=m.get("text", "")) for m in (markers or [])]
            if export == "edl":
                out = timeline.export_edl(kept, src_info["fps"],
                                          job.get("filename") or "video",
                                          f"{base}.edl", title=stem.upper()[:40])
            else:
                out = timeline.export_fcpxml(kept, src_info["fps"], src_info["width"],
                                             src_info["height"], job["video_path"],
                                             src_info["duration"], f"{base}.fcpxml",
                                             name=stem, markers=marker_objs,
                                             clip_name=os.path.basename(
                                                 job.get("filename") or "video.mp4"),
                                             audio=render.probe_audio(
                                                 job["video_path"]))
            ext = os.path.splitext(out)[1]
            job["outputs"][export] = {"path": out, "name": f"{stem}_tighten{ext}"}
            job["status"] = "ready"
            return

        ass_path = f"{base}.ass"
        with open(ass_path, "w", encoding="utf-8") as f:
            f.write(engine.build_ass(words, style, info["width"], info["height"],
                                     text_key=text_key))

        if export == "ass":
            out = ass_path
        elif export == "srt":
            out = render.export_srt(words, f"{base}.srt",
                                    style.words_per_line, text_key=text_key)
        elif export == "overlay":
            out = render.render_overlay(ass_path, f"{base}_overlay.mov",
                                        info["width"], info["height"],
                                        info["duration"], info["fps"])
        else:
            out = render.burn_video(source, ass_path, f"{base}_subtitled.mp4")

        ext = os.path.splitext(out)[1]
        suffix = {"burned": "_captioned", "overlay": "_overlay"}.get(export, "")
        job["outputs"][export] = {"path": out, "name": f"{stem}{suffix}{ext}"}
        job["status"] = "ready"
    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)[:500]


@app.post("/api/render", dependencies=WRITE)
async def render_endpoint(
    job_id: str = Form(...),
    style_json: str = Form(...),          # CaptionStyle JSON or {"preset": name, ...overrides}
    export: ExportFormat = Form("burned"),
    text_key: str = Form("hinglish"),
    transcript: Optional[str] = Form(None),  # edited transcript override
    cuts: Optional[str] = Form(None),        # JSON list of spans to remove
    markers: Optional[str] = Form(None),     # JSON list of flagged-but-kept spans
):
    job = _job(job_id)
    if not job.get("transcript"):
        raise HTTPException(status_code=409, detail="Transcribe first")

    try:
        style_data = json.loads(style_json)
        if not isinstance(style_data, dict):
            raise ValueError("style must be a JSON object")
        if "preset" in style_data:
            base = styles.STYLE_PRESETS.get(style_data["preset"])
            if base is None:
                raise ValueError(f"unknown preset {style_data['preset']}")
            overrides = {k: v for k, v in style_data.items() if k != "preset"}
            style = styles.CaptionStyle(**{**base.model_dump(), **overrides})
        else:
            style = styles.CaptionStyle(**style_data)
    except (json.JSONDecodeError, ValidationError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Bad style: {e}")

    try:
        if transcript:
            data = json.loads(transcript)
            if not isinstance(data, dict) or not isinstance(data.get("words"), list):
                raise ValueError("transcript must be an object with a words array")
            words = data["words"]
        else:
            words = job["transcript"]["words"]
        if not words:
            raise ValueError("empty transcript")
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Bad transcript: {e}")

    # Parsed separately so the message names the field that is actually wrong.
    try:
        cut_list = json.loads(cuts) if cuts else None
        if cut_list is not None and not isinstance(cut_list, list):
            raise ValueError("cuts must be a JSON array")
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Bad cuts: {e}")

    try:
        marker_list = json.loads(markers) if markers else None
        if marker_list is not None and not isinstance(marker_list, list):
            raise ValueError("markers must be a JSON array")
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Bad markers: {e}")

    threading.Thread(target=_render_worker,
                     args=(job_id, export, style, text_key, words, cut_list,
                           marker_list),
                     daemon=True).start()
    return {"job_id": job_id, "export": export}


# ── Status / download ────────────────────────────────────────────────────────

@app.get("/api/jobs/{job_id}")
async def job_status(job_id: str):
    job = _job(job_id)
    return {
        "status": job["status"],
        "error": job.get("error"),
        "filename": job.get("filename"),
        "transcript": job.get("transcript"),
        "video_info": job.get("video_info"),
        "outputs": {k: v["name"] for k, v in job["outputs"].items()},
        "retakes": _retakes_payload(job),
    }


@app.get("/api/download/{job_id}/{export}")
async def download(job_id: str, export: str):
    job = _job(job_id)
    out = job["outputs"].get(export)
    if not out or not os.path.exists(out["path"]):
        raise HTTPException(status_code=404, detail="Export not found")
    return FileResponse(out["path"], filename=out["name"])


# Bundled fonts served so the canvas preview can @font-face the same
# faces libass burns with.
FONTS_DIR = Path(__file__).parent / "fonts"
if FONTS_DIR.is_dir():
    app.mount("/fonts", StaticFiles(directory=str(FONTS_DIR)), name="fonts")
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
