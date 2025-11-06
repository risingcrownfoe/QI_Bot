import asyncio
import logging
from datetime import datetime, timedelta, date
from typing import Set

import discord

from qi_bot.config import settings
from qi_bot.schedule.loader import (
    load_schedule_if_changed,
    get_events_for_day,
    get_schedule_data,
)
from qi_bot.schedule.resolver import resolve_event, collect_files

log = logging.getLogger("qi-bot")

# Sent cache and guard flags live here
_sent_cache: Set[str] = set()
_sent_cache_lock = asyncio.Lock()
_current_date_str: str | None = None
_scheduler_started = False

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

TZ = ZoneInfo(settings.TIMEZONE)


def cycle_day_for(d: date) -> int:
    delta = (d - settings.CYCLE_START_DATE).days
    return (delta % settings.CYCLE_LENGTH) + 1


async def _ensure_channels(client: discord.Client):
    channels = []
    for cid in settings.ALLOWED_CHANNEL_IDS:
        ch = client.get_channel(cid)
        if ch is None:
            try:
                ch = await client.fetch_channel(cid)
            except Exception as e:
                log.error("[init] Could not fetch channel %s: %s", cid, e)
                continue
        channels.append(ch)
    if not channels:
        raise RuntimeError("No valid channels from ALLOWED_CHANNEL_IDS.")
    return channels


async def _send_event(channel: discord.abc.Messageable, when_dt: datetime, raw_event: dict, idx: int):
    event = resolve_event(raw_event, get_schedule_data())
    text = (event.get("text") or "").strip()
    files = [discord.File(fp) for fp in collect_files(event.get("image"))]

    key = f"{getattr(channel, 'id', 'unknown')}|{when_dt.date()}|{when_dt.strftime('%H:%M')}|{idx}"
    async with _sent_cache_lock:
        if key in _sent_cache:
            return
        _sent_cache.add(key)

    try:
        if files:
            await channel.send(content=text or None, files=files)
        else:
            await channel.send(content=text or "(no text)")
        log.info("[send] ✅ %s", key)
    except Exception as e:
        log.error("[send] ❌ %s", e)


async def _send_preview(channel: discord.abc.Messageable, raw_event: dict):
    """Legacy preview (kept for backward-compat). Adds a '(Preview send)' header."""
    event = resolve_event(raw_event, get_schedule_data())
    text = (event.get("text") or "").strip()
    files = [discord.File(fp) for fp in collect_files(event.get("image"))]
    try:
        await channel.send(content=f"*(Preview send)*\n{text}" if text else "*(Preview send — no text)*", files=files or None)
        log.info("[preview] ✅")
    except Exception as e:
        log.error("[preview] ❌ %s", e)


async def _send_full_now(channel: discord.abc.Messageable, raw_event: dict):
    """Send the full resolved message/content immediately, with NO extra header and NO sent-cache."""
    event = resolve_event(raw_event, get_schedule_data())
    text = (event.get("text") or "").strip()
    files = [discord.File(fp) for fp in collect_files(event.get("image"))]
    try:
        if files:
            await channel.send(content=text or None, files=files)
        else:
            await channel.send(content=text or "(kein Text)")
        log.info("[send-now] ✅")
    except Exception as e:
        log.error("[send-now] ❌ %s", e)


async def scheduler_loop(client: discord.Client):
    global _current_date_str, _sent_cache
    await client.wait_until_ready()
    channels = await _ensure_channels(client)
    load_schedule_if_changed(force=True)
    log.info("[init] Ready. Posting to %s", ", ".join(f"#{getattr(c, 'name', c.id)}" for c in channels))

    while not client.is_closed():
        try:
            load_schedule_if_changed()
            now = datetime.now(TZ)
            today = now.date()
            if _current_date_str != today.isoformat():
                async with _sent_cache_lock:
                    _sent_cache = set()
                _current_date_str = today.isoformat()
                log.info("[day] New day %s (Cycle %s)", today, cycle_day_for(today))

            events = get_events_for_day(cycle_day_for(today))
            for idx, ev in enumerate(events):
                hh, mm = map(int, ev["time"].split(":"))
                scheduled = datetime(today.year, today.month, today.day, hh, mm, tzinfo=TZ)
                if now >= scheduled and (now - scheduled) <= timedelta(minutes=settings.SEND_MISSED_WITHIN_MINUTES):
                    for ch in channels:
                        await _send_event(ch, scheduled, ev, idx)

            await asyncio.sleep(30)
        except Exception as e:
            log.exception("[loop] %s", e)
            await asyncio.sleep(5)


def start_scheduler(client: discord.Client):
    global _scheduler_started
    if not _scheduler_started:
        client.loop.create_task(scheduler_loop(client))
        _scheduler_started = True
        log.info("[init] Scheduler started.")


# Export helpers for commands
send_preview = _send_preview
send_full_now = _send_full_now
cycle_day_for_public = cycle_day_for
