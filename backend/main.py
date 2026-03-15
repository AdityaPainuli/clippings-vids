from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from typing import Optional, Dict
import os
import uuid
import asyncio
import clipper


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = "uploads"
OUTPUT_DIR = "output"
for d in [UPLOAD_DIR, OUTPUT_DIR]:
    if not os.path.exists(d):
        os.makedirs(d)

app.mount("/output", StaticFiles(directory=OUTPUT_DIR), name="output")

jobs: Dict[str, dict] = {}


def _make_clip_result(clip: dict) -> dict:
    """
    Return the clip with the video URL in every field name a frontend
    might use. Always a plain relative path — the frontend prepends its
    own origin, so we must NOT include a host here.
    """
    rel = f"/output/{clip['filename']}"
    return {
        **clip,
        "url":       rel,   # most common
        "video_url": rel,   # alternative
        "src":       rel,   # <video src=...>
        "path":      rel,   # legacy field
    }


async def process_video_task(
    job_id: str,
    video_path: str,
    instructions: Optional[str],
    info: Optional[dict] = None,
):
    loop = asyncio.get_event_loop()
    try:
        jobs[job_id]["status"] = "analyzing"
        clips_metadata = await loop.run_in_executor(
            None, clipper.analyze_video, video_path, instructions, info
        )

        jobs[job_id]["status"] = "clipping"
        clips = await loop.run_in_executor(
            None, clipper.create_clips, video_path, clips_metadata, OUTPUT_DIR
        )

        jobs[job_id]["status"] = "completed"
        jobs[job_id]["results"] = [_make_clip_result(c) for c in clips]

    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)


async def download_and_process(job_id: str, url: str, instructions: Optional[str]):
    loop = asyncio.get_event_loop()
    try:
        jobs[job_id]["status"] = "downloading"
        video_path, info = await loop.run_in_executor(
            None, clipper.download_video, url, UPLOAD_DIR
        )
        jobs[job_id]["video_path"] = video_path
        await process_video_task(job_id, video_path, instructions, info)
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)


@app.get("/")
async def root():
    return {"message": "Video Clipping API is running"}


@app.post("/process-url")
async def process_url(
    background_tasks: BackgroundTasks,
    url: str = Form(...),
    instructions: Optional[str] = Form(None),
):
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "queued", "url": url, "results": None, "error": None}
    background_tasks.add_task(download_and_process, job_id, url, instructions)
    return {"job_id": job_id, "status": "queued"}


@app.post("/upload")
async def upload_video(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    instructions: Optional[str] = Form(None),
):
    try:
        job_id = str(uuid.uuid4())
        file_path = os.path.join(UPLOAD_DIR, f"{job_id}_{file.filename}")
        with open(file_path, "wb") as buffer:
            buffer.write(await file.read())

        jobs[job_id] = {"status": "queued", "video_path": file_path, "results": None, "error": None}
        background_tasks.add_task(process_video_task, job_id, file_path, instructions, None)
        return {"job_id": job_id, "status": "uploaded"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status/{job_id}")
async def get_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs[job_id]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)