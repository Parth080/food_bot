"""Modal deployment entrypoint for the Janta Food Poll Bot.

Layout:
    * One always-warm web container (Flask + Slack Bolt as WSGI) — owns the
      in-memory vote buffer. Bounded to `max_containers=1` so the buffer has a
      single source of truth.
    * One cron function per scheduled IST time — runs in its own short-lived
      container and posts the poll via the Slack Web API.

First-time setup (one-shot):

    # 1. Install the Modal CLI and authenticate.
    pip install modal
    modal token new

    # 2. Create the secret Modal will inject into every container.
    #    Easiest: point at your existing .env (Modal supports --from-dotenv).
    modal secret create janta-poll-bot-secrets --from-dotenv .env

    # 3. Deploy.
    modal deploy modal_app.py

After deploy, Modal prints the public HTTPS URL of `web`. Configure the Slack
app's Request URL (events + interactivity) and Slash command URL to that URL
(use the same root URL for all three — the Flask routes already cover `/`,
`/slack/events`, `/slack/commands`).

Local dev still works via `python app.py` (Flask dev server, uses .env).
"""

from __future__ import annotations

import modal

# ── Image ────────────────────────────────────────────────────────────────────
# Pin Python and install deps from the repo's requirements.txt. The local
# source files listed below are baked into the image so they can be imported
# from inside the container.
image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install_from_requirements("requirements.txt")
    .add_local_python_source(
        "app",
        "poll",
        "vote_handler",
        "vote_buffer",
        "sheets",
        "poll_schedule_config",
    )
)

app = modal.App("janta-poll-bot", image=image)

# Single Modal secret that supplies every env var the app needs at runtime.
# Required keys: SLACK_BOT_TOKEN, SLACK_SIGNING_SECRET, SLACK_CHANNEL_ID,
# GOOGLE_SHEET_ID, GOOGLE_SA_PROJECT_ID, GOOGLE_SA_PRIVATE_KEY_ID,
# GOOGLE_SA_PRIVATE_KEY, GOOGLE_SA_CLIENT_EMAIL, GOOGLE_SA_CLIENT_ID
SECRETS = [modal.Secret.from_name("janta-poll-bot-secrets")]


# ── Web container ────────────────────────────────────────────────────────────
@app.function(
    secrets=SECRETS,
    cpu=2.0,
    memory=2048,
    min_containers=1,   # always-warm: zero cold-start for Slack's 3s ack window
    max_containers=1,   # single-owner of the in-memory vote buffer
    timeout=120,
)
@modal.concurrent(max_inputs=100)
@modal.wsgi_app()
def web():
    """Public HTTPS endpoint serving Slack events / commands / interactivity."""
    from app import flask_app, init_runtime
    init_runtime()
    return flask_app


# ── Scheduled polls (UTC cron — see poll_schedule_config.py for IST mapping) ─
# 17:00 Asia/Kolkata = 11:30 UTC
@app.function(
    secrets=SECRETS,
    cpu=0.5,
    memory=512,
    timeout=120,
    schedule=modal.Cron("30 11 * * *"),
)
def scheduled_poll_1700_ist():
    from app import post_scheduled_poll
    post_scheduled_poll("17:00")
