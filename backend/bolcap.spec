# PyInstaller spec for the Bolcap local app.
# Build: pyinstaller bolcap.spec  (from backend/)
# Output: dist/bolcap/ — onedir keeps startup fast and updates diffable.

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

datas = [("localapp/static", "localapp/static")]
# These packages ship data files PyInstaller misses on its own
datas += collect_data_files("ctranslate2")
datas += collect_data_files("faster_whisper", includes=["**/*.json", "assets/*"])
datas += collect_data_files("indic_transliteration")  # transliteration scheme JSONs

hiddenimports = (
    collect_submodules("uvicorn")
    + collect_submodules("faster_whisper")
    + ["multipart"]  # fastapi Form/File parsing (python-multipart)
)

a = Analysis(
    ["bolcap.py"],
    pathex=["."],
    datas=datas,
    hiddenimports=hiddenimports,
    excludes=[
        # Never bundled: heavy or server-only deps the local app doesn't use.
        # (av, onnxruntime, tokenizers stay — faster-whisper needs them.)
        "torch", "whisper", "mlx", "mlx_whisper",
        "supabase", "yt_dlp", "moviepy", "google.generativeai", "cv2",
        "transformers", "llvmlite", "numba", "nltk", "scipy", "pandas",
        "matplotlib", "PIL", "jedi", "IPython", "notebook", "sympy",
    ],
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    exclude_binaries=True,
    name="bolcap",
    console=True,   # visible console = visible errors; windowed build later
)

coll = COLLECT(exe, a.binaries, a.datas, name="bolcap")
