"""
Bolcap local app — everything on-device.

Thin localhost FastAPI server wrapping the captions engine, serving a
browser UI. No Supabase, no auth, no network processing: video, models,
and renders never leave the machine.

Run: python -m localapp
"""
