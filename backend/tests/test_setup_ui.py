import json
import os
import re
import shutil
import subprocess
import unittest
from pathlib import Path


HTML = Path(__file__).resolve().parents[1] / "localapp" / "static" / "index.html"


def _handler_source() -> str:
    text = HTML.read_text(encoding="utf-8")
    match = re.search(
        r'\$\("setup-btn"\)\.onclick = (async \(\) => \{.*?\n\});',
        text, re.DOTALL)
    if not match:
        raise AssertionError("setup button handler not found")
    return match.group(1)


def _run_handler(ok: bool, detail: str) -> dict:
    script = r"""
const handler = eval("(" + process.env.HANDLER + ")");
const els = {
  "setup-btn": { disabled: false },
  "setup-progress": { classList: { remove() {} } },
  "setup-progress-text": { textContent: "" },
};
global.$ = (id) => els[id];
global.chosenModel = "medium";
global.FormData = class { append() {} };
let intervalStarted = false;
global.setInterval = () => { intervalStarted = true; return 1; };
global.clearInterval = () => {};
global.fetch = async () => ({
  ok: process.env.OK === "1",
  json: async () => ({ detail: process.env.DETAIL }),
});
handler().then(() => {
  console.log(JSON.stringify({
    text: els["setup-progress-text"].textContent,
    disabled: els["setup-btn"].disabled,
    intervalStarted,
  }));
});
"""
    env = {**os.environ, "HANDLER": _handler_source(),
           "OK": "1" if ok else "0", "DETAIL": detail}
    done = subprocess.run(["node", "-e", script], capture_output=True,
                          text=True, env=env, timeout=10)
    if done.returncode != 0:
        raise AssertionError(done.stderr)
    return json.loads(done.stdout.strip())


@unittest.skipUnless(shutil.which("node"), "node is not installed")
class SetupButtonTest(unittest.TestCase):
    def test_failed_start_shows_the_server_message_and_stops(self):
        state = _run_handler(False, "Whisper model is not installed")
        self.assertIn("Could not start setup", state["text"])
        self.assertIn("Whisper model is not installed", state["text"])
        self.assertFalse(state["disabled"])
        self.assertFalse(state["intervalStarted"])

    def test_successful_start_begins_polling(self):
        state = _run_handler(True, "")
        self.assertTrue(state["intervalStarted"])
        self.assertTrue(state["disabled"])
        self.assertIn("Starting setup", state["text"])


if __name__ == "__main__":
    unittest.main()
