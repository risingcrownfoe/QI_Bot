import asyncio
import logging
from datetime import datetime, timedelta, date
from typing import Set

import discord

from qi_bot.config import settings, get_plan_for_channel
from qi_bot.schedule.loader import (
    load_schedule_if_changed,
    get_events_for_day,
    get_schedule_data,
)
from qi_bot.schedule.resolver import resolve_event, collect_files

log = logging.getLogger("qi-bot")

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


async def _ensure_channels_per_plan(client: discord.Client):
    """Resolve Discord channel objects for each plan.

    Returns a list of (plan, [channels...]) tuples.
    """
    plans_with_channels: list[tuple[object, list[discord.abc.Messageable]]] = []

    for plan in settings.SCHEDULE_PLANS:
        plan_channels: list[discord.abc.Messageable] = []
        for cid in plan.channel_ids:
            ch = client.get_channel(cid)
            if ch is None:
                try:
                    ch = await client.fetch_channel(cid)
                except Exception as e:
                    log.error("[init] Could not fetch channel %s for plan %s: %s", cid, plan.name, e)
                    continue
            plan_channels.append(ch)

        if not plan_channels:
            log.warning(
                "[init] No valid channels found for plan %s (%s)",
                plan.name,
                plan.schedule_file,
            )

        plans_with_channels.append((plan, plan_channels))

    return plans_with_channels


async def _send_event(
    channel: discord.abc.Messageable,
    when_dt: datetime,
    raw_event: dict,
    idx: int,
    schedule_file: str | None = None
):
    # If schedule_file not given (e.g. called from preview), infer from channel
    if schedule_file is None:
        plan = get_plan_for_channel(getattr(channel, "id", 0))
        schedule_file = plan.schedule_file if plan else None

    event = resolve_event(raw_event, get_schedule_data(schedule_file))
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

    plan = get_plan_for_channel(getattr(channel, "id", 0))
    schedule_file = plan.schedule_file if plan else None

    events = get_events_for_day(daynum, schedule_file=schedule_file)
    if not events:
        await channel.send(f"**Tag {daynum}**: keine Schritte geplant.")
        return

    header = f"**Tag {daynum} ({for_date.isoformat()}):** {len(events)} Schritte geplant.\n"
    await channel.send(header)

    for idx, ev in enumerate(events):
        dt = datetime(for_date.year, for_date.month, for_date.day, tzinfo=TZ)
        await _send_event(channel, dt, ev, idx, schedule_file=schedule_file)


async def _send_full_now(channel: discord.abc.Messageable, raw_event: dict):
    """Send a full event immediately (used by %now, %next, %step)."""
    plan = get_plan_for_channel(getattr(channel, "id", 0))
    schedule_file = plan.schedule_file if plan else None

    ev = resolve_event(raw_event, get_schedule_data(schedule_file))
    text = (ev.get("text") or "").strip()
    files = [discord.File(fp) for fp in collect_files(ev.get("image"))]

    if not text and not files:
        await channel.send("(Leere Nachricht – bitte Kursleitung informieren.)")
        return

    await channel.send(text, files=files)


async def scheduler_loop(client: discord.Client):
    global _current_date_str, _sent_cache

    await client.wait_until_ready()

    # Resolve channels per plan
    plans_with_channels = await _ensure_channels_per_plan(client)

    # For other purposes we can use the union of all schedule channels
    all_channels: list[discord.abc.Messageable] = [
        ch for _, chs in plans_with_channels for ch in chs
    ]

    # Resolve a dedicated snapshot/datascraper channel, if configured
    snapshot_channel: discord.abc.Messageable | None = None
    if settings.D1_STATUS_CHANNEL_ID is not None:
        cid = settings.D1_STATUS_CHANNEL_ID

        # Try to reuse any existing resolved channel first
        for ch in all_channels:
            if getattr(ch, "id", None) == cid:
                snapshot_channel = ch
                break

        # If not found yet, fetch it directly from Discord
        if snapshot_channel is None:
            ch = client.get_channel(cid)
            if ch is None:
                try:
                    ch = await client.fetch_channel(cid)
                except Exception as e:
                    log.error("[init] Could not fetch D1 status channel %s: %s", cid, e)
                    ch = None
            snapshot_channel = ch

    # Fallback: if no dedicated snapshot channel, use the first schedule channel (old behaviour)
    if snapshot_channel is None and all_channels:
        snapshot_channel = all_channels[0]

    # Initial load & logging per plan
    for plan, channels in plans_with_channels:
        load_schedule_if_changed(force=True, schedule_file=plan.schedule_file)
        log.info(
            "[init] Ready for plan %s (%s). Posting to %s",
            plan.name,
            plan.schedule_file,
            ", ".join(f"#{getattr(c, 'name', c.id)}" for c in channels),
        )

    while not client.is_closed():
        try:
            now = datetime.now(TZ)
            today = now.date()

            if _current_date_str != today.isoformat():
                async with _sent_cache_lock:
                    _sent_cache = set()
                _current_date_str = today.isoformat()
                log.info("[day] New day %s (Cycle %s)", today, cycle_day_for(today))

            daynum = cycle_day_for(today)

            # For each plan: load its file and send its events to its channels
            for plan, channels in plans_with_channels:
                if not channels:
                    continue

                load_schedule_if_changed(schedule_file=plan.schedule_file)

                events = get_events_for_day(daynum, schedule_file=plan.schedule_file)
                for idx, ev in enumerate(events):
                    time_str = ev.get("time")
                    if not time_str:
                        continue
                    try:
                        hh, mm = map(int, time_str.split(":"))
                    except Exception:
                        log.error("[loop] Bad time format in event: %r", time_str)
                        continue

                    scheduled = datetime(
                        today.year, today.month, today.day, hh, mm, tzinfo=TZ
                    )
                    if now >= scheduled and (now - scheduled) <= timedelta(
                        minutes=settings.SEND_MISSED_WITHIN_MINUTES
                    ):
                        for ch in channels:
                            await _send_event(
                                ch,
                                scheduled,
                                ev,
                                idx,
                                schedule_file=plan.schedule_file,
                            )

            # Daily FoE → D1 snapshot at 04:00 (use dedicated status channel if available)
            await _run_daily_snapshot_if_due(now, snapshot_channel)

            await asyncio.sleep(30)
        except Exception as e:
            log.exception("[loop] %s", e)
            await asyncio.sleep(5)

async def _run_daily_snapshot_if_due(
    now: datetime,
    target_channel: discord.abc.Messageable | None,
):

    """Fetch FoE data and push a daily snapshot into Cloudflare D1 at 04:00 local time."""
    # Compute today's 04:00 timestamp in TZ
    scheduled = datetime(now.year, now.month, now.day, 4, 0, tzinfo=TZ)
    key = f"d1-snapshot|{scheduled.date()}|04:00"

    # Only run within the grace window and once per day
    if not (
            now >= scheduled
            and (now - scheduled) <= timedelta(
        minutes=settings.SEND_MISSED_WITHIN_MINUTES
    )
    ):
        return

    async with _sent_cache_lock:
        if key in _sent_cache:
            return
        _sent_cache.add(key)

    await _run_snapshot_impl(target_channel, source="daily")


async def _run_snapshot_impl(
    target_channel: discord.abc.Messageable | None, source: str = "manual"
):
    """Core logic to fetch FoE data and insert a snapshot into D1.

    Used by both the daily scheduler and manual `%sql` command.
    """
    try:
        from qi_bot.utils.forge_scrape import fetch_players, build_daily_rows
        from qi_bot.utils.cloudfare_d1 import insert_daily_snapshot

        # Fetch + filter in a worker thread (blocking I/O)
        rows = await asyncio.to_thread(fetch_players)
        filtered_rows = await asyncio.to_thread(
            build_daily_rows, rows, 10_000, 5_000_000
        )

        result = await asyncio.to_thread(insert_daily_snapshot, filtered_rows)

        # ✅ SUCCESS MESSAGE
        if target_channel:
            label = result.get("label")
            count = result.get("rows_inserted")
            snapshot_id = result.get("snapshot_id")
            prefix = "✅ Daily FoE snapshot" if source == "daily" else "✅ Manual FoE snapshot"
            await target_channel.send(
                f"{prefix} stored in D1:\n"
                f"- Label: **{label}**\n"
                f"- Rows: **{count}**\n"
                f"- Snapshot ID: `{snapshot_id}`"
            )

        log.info(
            "[d1] ✅ Snapshot (%s) %s stored with %s rows",
            source,
            result.get("snapshot_id"),
            result.get("rows_inserted"),
        )

    except Exception as e:  # ❌ ERROR PATH
        log.exception("[d1] ❌ %s snapshot failed: %s", source, e)

        if target_channel:
            # Build a short, safe error description
            err_type = type(e).__name__
            err_msg = str(e)
            combined = f"{err_type}: {err_msg}"
            # avoid accidentally spamming Discord with a huge message
            combined = combined if len(combined) <= 1500 else combined[:1497] + "..."

            try:
                await target_channel.send(
                    f"❌ { 'Daily' if source == 'daily' else 'Manual' } FoE snapshot **FAILED**.\n"
                    f"Error: `{combined}`\n"
                    "Check Render logs for full stack trace."
                )
            except Exception as send_err:
                log.error("[d1] Could not send error message to Discord: %s", send_err)


async def run_manual_snapshot(target_channel: discord.abc.Messageable):
    """Public helper: run a manual FoE → D1 snapshot now, reporting to the given channel."""
    await _run_snapshot_impl(target_channel, source="manual")


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
# new export for commands
run_manual_snapshot_public = run_manual_snapshot
