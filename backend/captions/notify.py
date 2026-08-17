"""
Completion notifications.

Two channels, both fired when a job finishes (or fails):
  1. In-app feed — the job row's `seen` flag; frontend polls
     GET /captions/notifications for a bell badge. Always on.
  2. Email — via Resend (RESEND_API_KEY + CAPTION_FROM_EMAIL env).
     Skipped silently when not configured, so local dev never breaks.

Emails include the signed download link, which stays valid for the full
retention window — recipients can download without logging back in.
"""

import os

import requests

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
FROM_EMAIL     = os.getenv("CAPTION_FROM_EMAIL", "captions@notifications.local")
APP_URL        = os.getenv("APP_URL", "http://localhost:3000")

RETENTION_HOURS = int(os.getenv("CAPTION_RETENTION_SECONDS", 48 * 3600)) // 3600


def _send_email(to: str, subject: str, html: str) -> bool:
    if not RESEND_API_KEY:
        return False
    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}",
                     "Content-Type": "application/json"},
            json={"from": FROM_EMAIL, "to": [to], "subject": subject, "html": html},
            timeout=15,
        )
        return resp.status_code in (200, 201)
    except requests.RequestException as e:
        print(f"  [notify] Email send failed: {e}")
        return False


def notify_completed(email: str, job_id: str, filename: str,
                     download_url: str | None) -> bool:
    link = (f'<p><a href="{download_url}">Download {filename}</a></p>'
            if download_url else
            f'<p>Open <a href="{APP_URL}">the app</a> to download it.</p>')
    html = (
        f"<p>Your captioned file <strong>{filename}</strong> is ready.</p>"
        f"{link}"
        f"<p>It will be deleted after {RETENTION_HOURS} hours — "
        f"download it before then.</p>"
    )
    sent = _send_email(email, f"Your captions are ready — {filename}", html)
    if sent:
        print(f"  [notify] Completion email sent to {email} (job {job_id})")
    return sent


def notify_failed(email: str, job_id: str, error: str) -> bool:
    html = (
        f"<p>Your caption job failed: {error[:300]}</p>"
        f'<p>No credits were consumed. Try again from <a href="{APP_URL}">the app</a>.</p>'
    )
    return _send_email(email, "Caption job failed", html)
