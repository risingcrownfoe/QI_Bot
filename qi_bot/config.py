# qi_bot/config.py

import os
from dataclasses import dataclass
from datetime import date

from dotenv import load_dotenv

# Load .env just once, here.
load_dotenv()

@dataclass(frozen=True)
class Settings:
    # Secrets (env)
    DISCORD_TOKEN: str
    HEALTH_URL: str | None

    # Non-secrets (code defaults)
    TIMEZONE: str
    CYCLE_START_DATE: date
    CYCLE_LENGTH: int
    SEND_MISSED_WITHIN_MINUTES: int
    SCHEDULE_FILE: str
    ALLOWED_CHANNEL_IDS: tuple[int, ...]
    PORT: int

# Read the two secrets from env; everything else is coded.
_discord = os.getenv("DISCORD_TOKEN")
if not _discord:
    raise RuntimeError("Missing DISCORD_TOKEN in .env")

_health_url = os.getenv("HEALTH_URL")  # optional; we also fall back to RENDER_EXTERNAL_URL at runtime

# NOTE: The IDs below are not secrets. Pulled from your current .env for identical behavior.
DEFAULT_ALLOWED_CHANNEL_IDS = (
    1432327080749568000,
    1433087490587230349,
)

settings = Settings(
    DISCORD_TOKEN=_discord,
    HEALTH_URL=_health_url,
    TIMEZONE="Europe/Zurich",
    CYCLE_START_DATE=date.fromisoformat("2025-10-16"),
    CYCLE_LENGTH=14,
    SEND_MISSED_WITHIN_MINUTES=10,
    SCHEDULE_FILE="messages.json",
    ALLOWED_CHANNEL_IDS=DEFAULT_ALLOWED_CHANNEL_IDS,
    PORT=10000,
)
