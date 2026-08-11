"""Melonbot runtime config.

Secrets and per-machine flags live in `.env` (see `.env.example`).
Flip MELONBOT_DB and USE_SSH_TUNNEL at the top of `.env`; everything else follows.
"""

import os
from pathlib import Path

_ENV_PATH = Path(__file__).resolve().parent / ".env"

try:
    from dotenv import load_dotenv

    # Always load repo-root .env (not cwd). override=False keeps real shell exports.
    loaded = load_dotenv(_ENV_PATH, override=False)
    if not loaded and not _ENV_PATH.is_file():
        print(f"warning: no .env at {_ENV_PATH}", flush=True)
except ImportError:
    print(
        "warning: python-dotenv not installed; .env will not be loaded. "
        "pip install python-dotenv",
        flush=True,
    )


def _flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(
            f"Missing required env var {name!r}. "
            "Copy .env.example to .env and fill in values."
        )
    return value


# ---------------------------------------------------------------------------
# Controls (mirror top of .env)
# ---------------------------------------------------------------------------
# MELONBOT_DB: 'dev' | 'prod' → picks DB name + bot token + command prefix
# USE_SSH_TUNNEL: False = bot on DB server; True = SSH from elsewhere
DB_TARGET = (os.environ.get("MELONBOT_DB") or "dev").strip().lower()
if DB_TARGET not in ("dev", "prod"):
    raise RuntimeError(f"MELONBOT_DB must be 'dev' or 'prod', got {DB_TARGET!r}")

USE_SSH_TUNNEL = _flag("USE_SSH_TUNNEL")

# Avoid colliding with prod: default "?" for dev, "!" for prod. Override via env if needed.
if DB_TARGET == "prod":
    COMMAND_PREFIX = os.environ.get("PROD_COMMAND_PREFIX", "!").strip() or "!"
else:
    COMMAND_PREFIX = os.environ.get("DEV_COMMAND_PREFIX", "?").strip() or "?"

# ---------------------------------------------------------------------------
# How to reach Postgres
# REMOTE_* = Postgres on the SERVER (almost always loopback).
# SSH_* read by db_tunnel when USE_SSH_TUNNEL is set.
# ---------------------------------------------------------------------------
REMOTE_DB_HOST = os.environ.get("REMOTE_DB_HOST", "127.0.0.1")
REMOTE_DB_PORT = int(os.environ.get("REMOTE_DB_PORT", "5432"))

if USE_SSH_TUNNEL:
    import db_tunnel

    db_tunnel.open_tunnel(REMOTE_DB_HOST, REMOTE_DB_PORT)
    DB_HOST = "127.0.0.1"
    DB_PORT = str(db_tunnel.local_bind_port())
else:
    DB_HOST = REMOTE_DB_HOST
    DB_PORT = str(REMOTE_DB_PORT)

# ---------------------------------------------------------------------------
# Selected credentials (from MELONBOT_DB)
# ---------------------------------------------------------------------------
admin_discord_ids = [117340965760532487]

if DB_TARGET == "prod":
    bot_token = _require("PROD_BOT_TOKEN")
    DB_NAME = os.environ.get("PROD_DB_NAME", "melonbot")
    DB_USER = os.environ.get("PROD_DB_USER", "postgres")
    DB_PASSWORD = os.environ.get("PROD_DB_PASSWORD", "")
else:
    bot_token = _require("DEV_BOT_TOKEN")
    DB_NAME = os.environ.get("DEV_DB_NAME", "melonbot_dev")
    DB_USER = os.environ.get("DEV_DB_USER", "postgres")
    DB_PASSWORD = os.environ.get("DEV_DB_PASSWORD", "")

if not DB_PASSWORD:
    raise RuntimeError(
        f"Set {'PROD' if DB_TARGET == 'prod' else 'DEV'}_DB_PASSWORD in .env"
    )

PSQL_CREDENTIALS = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "host": DB_HOST,
    "port": DB_PORT,
    "database": DB_NAME,
}

# ---------------------------------------------------------------------------
# Other services
# ---------------------------------------------------------------------------
gapikey = os.environ.get("GOOGLE_API_KEY", "")
gcsekey = os.environ.get("GOOGLE_CSE_KEY", "")
google_narrate_key = os.environ.get("GOOGLE_NARRATE_KEY", "")

SFTP_CREDENTIALS = {
    "host": os.environ.get("SFTP_HOST", ""),
    "port": int(os.environ.get("SFTP_PORT", "22")),
    "username": os.environ.get("SFTP_USER", ""),
    "password": os.environ.get("SFTP_PASSWORD", ""),
    "base_dir": os.environ.get("SFTP_BASE_DIR", "streams"),
}

LOCAL_STREAM_FILE_LOCATION = os.environ.get(
    "LOCAL_STREAM_FILE_LOCATION", "/mnt/md0/nitwitch_dl/streams"
)
WEB_STREAM_FILE_LOCATION = os.environ.get(
    "WEB_STREAM_FILE_LOCATION", "https://nitwitch.com/dl/streams"
)

twitch_client_id = os.environ.get("TWITCH_CLIENT_ID", "")
twitch_client_secret = os.environ.get("TWITCH_CLIENT_SECRET", "")
twitch_refresh_token = os.environ.get("TWITCH_REFRESH_TOKEN", "")

STREAMLINK_BIN = os.environ.get(
    "STREAMLINK_BIN", "D:/code/melonbot/venv/Scripts/streamlink.exe"
)

# ---------------------------------------------------------------------------
# Message archive (stealth; no Discord commands)
# ---------------------------------------------------------------------------
MESSAGE_ARCHIVE_ENABLED = _flag("MESSAGE_ARCHIVE_ENABLED")


def _parse_guild_ids(raw: str) -> frozenset[int]:
    ids: set[int] = set()
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            print(f"warning: ignoring invalid MESSAGE_ARCHIVE_GUILD_IDS entry {part!r}", flush=True)
    return frozenset(ids)


MESSAGE_ARCHIVE_GUILD_IDS = _parse_guild_ids(
    os.environ.get("MESSAGE_ARCHIVE_GUILD_IDS", "")
)
MESSAGE_ARCHIVE_STATUS_PATH = os.environ.get(
    "MESSAGE_ARCHIVE_STATUS_PATH", "data/message_archive_status.json"
).strip() or "data/message_archive_status.json"
