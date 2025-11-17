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

    if not text and not files:
        log.warning("[send_event] Empty text + no files for event %s", event)
        return

    try:
        await channel.send(text, files=files)
    except Exception as e:
        log.error("[send_event] Failed to send message: %s", e)


async def _send_preview(channel: discord.abc.Messageable, for_date: date):
    """Used by %today and %day to preview what will be sent on a given date."""
    daynum = cycle_day_for(for_date)
    events = get_events_for_day(daynum)
    if not events:
        await channel.send(f"**Tag {daynum}**: keine Schritte geplant.")
        return
    header = f"**Tag {daynum} ({for_date.isoformat()}):** {len(events)} Schritte geplant.\n"
    await channel.send(header)
    # Then send each full event one after another
    for idx, ev in enumerate(events):
        dt = datetime(for_date.year, for_date.month, for_date.day, tzinfo=TZ)
        await _send_event(channel, dt, ev, idx)


async def _send_full_now(channel: discord.abc.Messageable, raw_event: dict):
    """Send a full event immediately (used by %now, %next, %step)."""
    ev = resolve_event(raw_event, get_schedule_data())
    text = (ev.get("text") or "").strip()
    files = [discord.File(fp) for fp in collect_files(ev.get("image"))]

    if not text and not files:
        await channel.send("(Leere Nachricht – bitte Kursleitung informieren.)")
        return

    await channel.send(text, files=files)


async def scheduler_loop(client: discord.Client):
    global _current_date_str, _sent_cache
    await client.wait_until_ready()
    channels = await _ensure_channels(client)
    load_schedule_if_changed(force=True)
    log.info(
        "[init] Ready. Posting to %s",
        ", ".join(f"#{getattr(c, 'name', c.id)}" for c in channels),
    )

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
                time_str = ev.get("time")
                if not time_str:
                    continue
                try:
                    hh, mm = map(int, time_str.split(":"))
                except Exception:
                    log.error("[loop] Bad time format in event: %r", time_str)
                    continue
                scheduled = datetime(today.year, today.month, today.day, hh, mm, tzinfo=TZ)
                if now >= scheduled and (now - scheduled) <= timedelta(
                    minutes=settings.SEND_MISSED_WITHIN_MINUTES
                ):
                    for ch in channels:
                        await _send_event(ch, scheduled, ev, idx)

            # New: run the daily D1 snapshot at ~04:00 local time
            await _run_daily_snapshot_if_due(channels, now)

            await asyncio.sleep(30)
        except Exception as e:
            log.exception("[loop] %s", e)
            await asyncio.sleep(5)


async def _run_daily_snapshot_if_due(channels, now: datetime):
    """Fetch FoE data and push a daily snapshot into Cloudflare D1 at 04:00 local time.

    This replaces the former CSV/GitHub pipeline. We still respect the same
    grace window (SEND_MISSED_WITHIN_MINUTES) and only run once per day.
    """
    # Compute today's 04:00 timestamp in TZ
    scheduled = datetime(now.year, now.month, now.day, 4, 0, tzinfo=TZ)
    key = f"d1-snapshot|{scheduled.date()}|04:00"

    # Only run within the grace window and once per day
    if now >= scheduled and (now - scheduled) <= timedelta(
        minutes=settings.SEND_MISSED_WITHIN_MINUTES
    ):
        async with _sent_cache_lock:
            if key in _sent_cache:
                return
            _sent_cache.add(key)

        # Import here so that normal bot usage does not pay the import cost
        try:
            import asyncio
            from qi_bot.utils.forge_scrape import fetch_players, build_daily_rows
            from qi_bot.utils.cloudflare_d1 import insert_daily_snapshot

            # Fetch + filter in a worker thread (blocking I/O)
            rows = await asyncio.to_thread(fetch_players)
            filtered_rows = await asyncio.to_thread(
                build_daily_rows, rows, 10_000, 5_000_000
            )

            result = await asyncio.to_thread(insert_daily_snapshot, filtered_rows)

            # Send a brief confirmation to the first configured channel (if any)
            if channels:
                ch = channels[0]
                try:
                    label = result.get("label")
                    count = result.get("rows_inserted")
                    snapshot_id = result.get("snapshot_id")
                    await ch.send(
                        f"Daily FoE snapshot stored in D1: **{label}** "
                        f"(rows: {count}, snapshot_id: {snapshot_id})"
                    )
                except Exception as send_err:
                    log.error("[d1] Could not send confirmation message: %s", send_err)

            log.info(
                "[d1] ✅ Snapshot %s stored with %s rows",
                result.get("snapshot_id"),
                result.get("rows_inserted"),
            )
        except Exception as e:  # pragma: no cover - defensive
            log.exception("[d1] ❌ %s", e)


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
