#!/usr/bin/env bash
# Bolcap release smoke test — exercises the real pipeline, not just endpoints:
# first-run setup (ffmpeg/ffprobe/model downloads through the app's own
# downloader), transcription of a generated fixture, and a burned render
# validated with ffprobe. Publishing without this passing would ship a
# binary that boots but can't process video.
set -euo pipefail

APP_BIN="${1:-./dist/bolcap/bolcap}"
BASE="http://127.0.0.1:8756"
export BOLCAP_HOME="${BOLCAP_HOME:-$(mktemp -d)/bolcap-home}"

say() { echo "[smoke] $*"; }

"$APP_BIN" &
APP_PID=$!
trap 'kill $APP_PID 2>/dev/null || true' EXIT

for i in $(seq 1 30); do
  curl -sf "$BASE/api/presets" >/dev/null 2>&1 && break
  sleep 1
done
curl -sf "$BASE/api/presets" | grep -q bold_impact
say "server up"

# ── First-run setup through the app's own downloader (tiny model for CI) ─────
curl -sf -X POST -F "model=tiny" "$BASE/api/setup/run" >/dev/null
for i in $(seq 1 120); do
  STATUS=$(curl -sf "$BASE/api/setup/status" | python3 -c \
    "import json,sys; print(json.load(sys.stdin)['progress'].get('status',''))")
  [ "$STATUS" = "ready" ] && break
  if [ "$STATUS" = "failed" ]; then
    curl -sf "$BASE/api/setup/status"; echo; say "setup FAILED"; exit 1
  fi
  sleep 5
done
[ "$STATUS" = "ready" ] || { say "setup timed out"; exit 1; }
say "setup ready"

# ── Generate a 3s vertical fixture (ffmpeg now guaranteed present) ───────────
FFMPEG=$(command -v ffmpeg || echo "$BOLCAP_HOME/bin/ffmpeg")
FIXTURE=$(mktemp -d)/fixture.mp4
"$FFMPEG" -y -f lavfi -i "testsrc=size=540x960:rate=30:duration=3" \
  -f lavfi -i "sine=frequency=440:duration=3" \
  -c:v libx264 -pix_fmt yuv420p -c:a aac "$FIXTURE" 2>/dev/null
say "fixture generated"

# ── Transcribe (validates ffmpeg + ffprobe + whisper model inside the app) ───
JOB=$(curl -sf -F "file=@$FIXTURE" "$BASE/api/transcribe" | python3 -c \
  "import json,sys; print(json.load(sys.stdin)['job_id'])")
for i in $(seq 1 60); do
  S=$(curl -sf "$BASE/api/jobs/$JOB" | python3 -c \
    "import json,sys; print(json.load(sys.stdin)['status'])")
  [ "$S" = "ready" ] && break
  [ "$S" = "failed" ] && { curl -sf "$BASE/api/jobs/$JOB"; echo; say "transcribe FAILED"; exit 1; }
  sleep 3
done
[ "$S" = "ready" ] || { say "transcribe timed out"; exit 1; }
say "transcription ok"

# ── Burned render with a reference transcript (validates engine + fonts) ─────
TRANSCRIPT='{"words":[{"start":0.2,"end":0.9,"text":"नमस्ते","hinglish":"namaste"},{"start":1.0,"end":1.8,"text":"बोलकैप","hinglish":"bolcap"},{"start":1.9,"end":2.6,"text":"testing","hinglish":"testing"}]}'
curl -sf -F "job_id=$JOB" -F "style_json={\"preset\":\"bold_impact\"}" \
  -F "export=burned" -F "text_key=hinglish" -F "transcript=$TRANSCRIPT" \
  "$BASE/api/render" >/dev/null
for i in $(seq 1 60); do
  DONE=$(curl -sf "$BASE/api/jobs/$JOB" | python3 -c \
    "import json,sys; j=json.load(sys.stdin); print('yes' if 'burned' in j['outputs'] else j['status'])")
  [ "$DONE" = "yes" ] && break
  [ "$DONE" = "failed" ] && { curl -sf "$BASE/api/jobs/$JOB"; echo; say "render FAILED"; exit 1; }
  sleep 3
done
[ "$DONE" = "yes" ] || { say "render timed out"; exit 1; }

OUT=$(mktemp -d)/out.mp4
curl -sf -o "$OUT" "$BASE/api/download/$JOB/burned"
[ "$(wc -c < "$OUT")" -gt 10000 ] || { say "output suspiciously small"; exit 1; }
FFPROBE=$(command -v ffprobe || echo "$BOLCAP_HOME/bin/ffprobe")
"$FFPROBE" -v error -show_entries stream=codec_name "$OUT" | grep -q h264
say "burned render ok — smoke test PASSED"
