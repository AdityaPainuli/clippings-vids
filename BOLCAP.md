<img src="backend/localapp/icons/bolcap.png" width="96" align="right" alt="Bolcap icon">

# Bolcap

Animated Hinglish captions that spell words the way your audience types them.
Everything runs on your machine: your video, the transcription, and the render
never touch a server.

<img src="docs/demo-captions.gif" width="220" align="right" alt="Animated captions on a vertical video">

- **Hinglish, done properly.** Whisper transcribes Hindi/English code-switched
  speech, then the words are romanized the way people actually write them
  ("chunautiyan", not the dictionary's "cunatiyam").
- **Fix what the model got wrong.** Click a word to jump the video there,
  double-click to retype it. No re-transcription.
- **Style it your way.** Font, size, colors, position, words per line, and four
  animations (static highlight, pop-in, fade, karaoke fill) — with a live
  preview over your own footage.
- **Export how you edit.** Burned-in MP4 for social, or a transparent overlay
  `.mov` you drop straight onto a Premiere / Final Cut / Resolve timeline
  without re-encoding your footage. Plus `.srt` and `.ass`.

<br clear="right">

![Bolcap window](docs/ui.jpg)

## Install

Grab the zip for your machine from the
[latest release](https://github.com/AdityaPainuli/clippings-vids/releases/latest),
unzip it, and run `bolcap`. It starts a local server and opens the app in your
browser.

| Download | For | Needs |
|---|---|---|
| `bolcap-macos-arm64.zip` | Macs with Apple Silicon (M1 and later) | **macOS 14 Sonoma or later** |
| `bolcap-macos-x64.zip` | Intel Macs | **macOS 14 Sonoma or later** |
| `bolcap-windows-x64.zip` | Windows | Windows 10 or 11 (64-bit) |
| `bolcap-linux-x64.zip` | Linux (x86_64) | **glibc 2.38+** — Ubuntu 24.04, Debian 13, Fedora 39 or newer |

Those minimums are measured from the shipped binaries, not guessed: numpy and
onnxruntime are compiled against macOS 14 on both Mac architectures, and the
bundled CPython needs `GLIBC_2.38`. Check yours with **Apple menu → About This
Mac**, or `ldd --version` on Linux.

An Intel Mac from 2017 or earlier cannot run macOS 14 at all, so it cannot run
Bolcap. Ubuntu 22.04 LTS ships glibc 2.35 and is too old.

If your machine is below the line, Bolcap says so on the first screen rather
than letting you pick a video and download a model first.

### Getting past the OS warning

Bolcap isn't code-signed yet — signing costs money per year and this is a free
tool — so both operating systems will warn you about an unidentified developer.
The binaries are built in public by
[this workflow](.github/workflows/bolcap-release.yml) straight from this repo,
and you can read every line of what runs.

- **macOS**: `bolcap` is a plain executable rather than a `.app`, and macOS 15
  Sequoia removed the old Control-click → Open shortcut for unsigned software,
  so the reliable route is to strip the download flag in Terminal:

  ```bash
  xattr -dr com.apple.quarantine /path/to/bolcap
  ```

  Or try to open it once, then go to **System Settings → Privacy & Security**
  and press **Open Anyway** next to the message about `bolcap`. Either way you
  only do it once.
- **Windows**: SmartScreen shows "Windows protected your PC" → **More info** →
  **Run anyway**.

### First launch

Bolcap downloads what it needs the first time you run it — a Whisper model and,
if you don't already have them, `ffmpeg` and `ffprobe`. Everything lands in
`~/.bolcap` and is verified against a pinned checksum. Interrupted downloads
resume where they left off.

Pick your model with your hardware in mind:

| Model | Download | Honest expectation |
|---|---|---|
| `small` | ~500 MB | Fastest, and fine for English — but it mangles Hindi. On our reference clip it produced "kisi bitar ka visakar" where the bigger models got "kisi bhi tarah ka avishkar". |
| `medium` | ~1.5 GB | **Recommended for Hinglish**, and the default. Slow on a CPU-only machine. |
| `large-v3` | ~3 GB | Best accuracy. Bring a GPU or patience. |

You can always fix words by hand in the transcript editor, but starting from a
better transcript means fixing far fewer of them.

Rough timing: a CPU-only laptop transcribes roughly half a minute of audio per
minute of waiting on `small`. An NVIDIA GPU is many times faster and is picked
up automatically.

## Using it

1. Drop in a video. If the speech isn't English, set **Spoken language** first —
   auto-detect gets it wrong on noisy or very short clips, and a wrong guess
   produces a nonsense transcript.
2. Wait for the transcript. This is the slow part; everything after it is fast.
3. Fix any words the model misheard, and switch between Hinglish and Devanagari
   if you'd rather burn the original script.
4. Pick a preset, then adjust anything you like. The preview updates live.
5. Export. Use **Overlay .mov** if you're finishing in an editor; use
   **Captioned MP4** if you're posting straight to social.

## Notes

- Nothing is uploaded. The app binds to `127.0.0.1` and has no accounts.
- Working files live in `~/.bolcap/work` and are cleaned up after three days.
- Bolcap is Apache-2.0 licensed — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
- Bundled fonts (Archivo Black, Inter, Noto Sans Devanagari) are OFL-licensed;
  see [`backend/localapp/fonts/LICENSE.md`](backend/localapp/fonts/LICENSE.md).
- ffmpeg isn't shipped inside the app — it's downloaded to your machine on
  first run (or your existing one is used) and called as a separate program.
- Prefer not to install anything? The same engine runs as a hosted web app —
  same captions, no setup, and transcription on a GPU instead of your laptop.

## Running from source

```bash
cd backend
pip install -r requirements-bolcap.txt
python -m localapp
```

Or skip the interface entirely:

```bash
python -m captions.cli video.mp4 --export overlay
python -m captions.cli video.mp4 --transcript saved.json --preset karaoke
```
