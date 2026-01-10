# qi_bot/config.py

import os
from dataclasses import dataclass
from datetime import date

from dotenv import load_dotenv

# Load .env just once, here.
load_dotenv()


@dataclass(frozen=True)
class SchedulePlan:
    """A plan = schedule file + the channels that use it."""
    name: str
    schedule_file: str
    channel_ids: tuple[int, ...]


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

    # New: all schedule plans
    SCHEDULE_PLANS: tuple[SchedulePlan, ...]

    # New: dedicated channel for the 04:00 FoE/D1 datascraper status
    # Set this to the numeric Discord channel ID you want, or None to disable.
    D1_STATUS_CHANNEL_ID: int | None


# Read the two secrets from env; everything else is coded.
_discord = os.getenv("DISCORD_TOKEN")
if not _discord:
    raise RuntimeError("Missing DISCORD_TOKEN in .env")

_health_url = os.getenv("HEALTH_URL")  # optional; we also fall back to RENDER_EXTERNAL_URL at runtime


DEFAULT_SCHEDULE_PLANS: tuple[SchedulePlan, ...] = (
    SchedulePlan(
        name="alch",
        schedule_file="messages_alch.json",
        channel_ids=(
            1459513547867295918,    # single DC
            1453801024727814275,    # RC
        ),
    ),
    SchedulePlan(
        name="linnun",
        schedule_file="messages_linnun_short.json",
        channel_ids=(
            1432327080749568000,    # single DC
            1433087490587230349,    # RC
        ),
    ),
)

# Flatten all channel IDs from all plans
DEFAULT_ALLOWED_CHANNEL_IDS = tuple(
    sorted(
        {
            cid
            for plan in DEFAULT_SCHEDULE_PLANS
            for cid in plan.channel_ids
        }
    )
)

DEFAULT_SCHEDULE_FILE = (
    DEFAULT_SCHEDULE_PLANS[0].schedule_file
    if DEFAULT_SCHEDULE_PLANS
    else "messages_alch.json"
)

settings = Settings(
    DISCORD_TOKEN=_discord,
    HEALTH_URL=_health_url,
    TIMEZONE="Europe/Zurich",
    CYCLE_START_DATE=date.fromisoformat("2025-10-16"),
    CYCLE_LENGTH=14,
    SEND_MISSED_WITHIN_MINUTES=10,
    SCHEDULE_FILE=DEFAULT_SCHEDULE_FILE,
    ALLOWED_CHANNEL_IDS=DEFAULT_ALLOWED_CHANNEL_IDS,
    PORT=10000,
    SCHEDULE_PLANS=DEFAULT_SCHEDULE_PLANS,
    D1_STATUS_CHANNEL_ID=1459513631271031028,
)

# Quick lookup: channel_id → plan
CHANNEL_ID_TO_PLAN = {
    cid: plan
    for plan in settings.SCHEDULE_PLANS
    for cid in plan.channel_ids
}


def get_plan_for_channel(channel_id: int) -> SchedulePlan | None:
    return CHANNEL_ID_TO_PLAN.get(channel_id)
