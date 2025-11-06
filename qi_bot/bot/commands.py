import logging
from datetime import datetime, timedelta

import discord

from qi_bot.config import settings
from qi_bot.scheduler.loop import (
    start_scheduler,
    send_full_now,
    cycle_day_for_public,
)
from qi_bot.schedule.loader import (
    load_schedule_if_changed,
    get_events_for_day,
)

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

log = logging.getLogger("qi-bot")
TZ = ZoneInfo(settings.TIMEZONE)

# -------- Helpers --------

def _norm_events_for_day_struct(day_struct):
    """Accepts either a list (old shape) or an object {title?, notes?, events: []}; returns events list + optional day title/notes."""
    day_title = None
    day_notes = None
    if isinstance(day_struct, dict):
        events = day_struct.get("events", [])
        day_title = day_struct.get("title")
        day_notes = day_struct.get("notes")
    else:
        events = day_struct or []
    return events, day_title, day_notes


def _all_days_iter(schedule_data):
    """Yield (day_index:int, events:list, day_title:str|None). Days sorted by number."""
    days = schedule_data.get("days", {})
    for d_key in sorted(days, key=lambda x: int(x)):
        evs, day_title, _ = _norm_events_for_day_struct(days[d_key])
        yield int(d_key), evs, day_title


def _fmt_time_and_optional_title(ev, idx=None):
    """Return 'N. HH:MM Uhr: <title>' or 'HH:MM Uhr: ' (no title) according to request."""
    hhmm = ev.get("time", "??:??")
    title = ev.get("title")  # may be None
    prefix = f"{idx}. " if idx is not None else ""
    if title:
        return f"{prefix}{hhmm} Uhr: {title}"
    else:
        return f"{prefix}{hhmm} Uhr:"


def _find_now_and_next_for_today(now_dt):
    """Return (latest_ev_or_None, next_ev_or_None, today_daynum)."""
    today = now_dt.date()
    daynum = cycle_day_for_public(today)
    events = get_events_for_day(daynum)
    # events are sorted by time string already
    latest = None
    nxt = None
    for ev in events:
        hh, mm = map(int, ev["time"].split(":"))
        ev_dt = now_dt.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if ev_dt <= now_dt:
            latest = ev
        else:
            nxt = ev
            break
    return latest, nxt, daynum


def _find_first_event_after_today(now_dt):
    """Find the next day with events after today; scan up to one full cycle."""
    for step in range(1, settings.CYCLE_LENGTH + 1):
        dt = now_dt + timedelta(days=step)
        dnum = cycle_day_for_public(dt.date())
        evs = get_events_for_day(dnum)
        if evs:
            return dnum, evs[0]
    return None, None


# -------- Command registry --------

# Visible command pairs (left = EN, right = DE)
VISIBLE_ALIASES = [
    ("%help", "%hilfe"),
    ("%today", "%heute"),
    ("%day", "%tag"),
    ("%all", "%alle"),
    ("%now", "%jetzt"),
    ("%next", "%nächster"),
    ("%step", "%schritt"),
]

# Hidden synonyms that are accepted but NOT shown in help
HIDDEN_ALIASES = {
    "%naechster": "%next",  # maps to %next
}

# Map of any trigger to the canonical command key
CANONICAL = {
    "%help": "help", "%hilfe": "help",
    "%today": "today", "%heute": "today",
    "%day": "day", "%tag": "day",
    "%all": "all", "%alle": "all",
    "%now": "now", "%jetzt": "now",
    "%next": "next", "%nächster": "next", "%naechster": "next",
    "%step": "step", "%schritt": "step",
}

HELP_LINES = [
    "%today / %heute — gibt alle für heute geplanten Schritte aus.",
    "%day <d> / %tag <d> — zeigt alle Schritte für Tag d.",
    "%all / %alle — zeigt alle geplanten Schritte (Zeit + Titel) für den gesamten Zyklus.",
    "%now / %jetzt — zeigt den zuletzt fälligen Schritt von heute.",
    "%next / %nächster — zeigt den nächsten Schritt.",
    "%step <d> <n> / %schritt <d> <n> — zeigt den n-ten Schritt von Tag d (voller Text).",
    "%help / %hilfe — zeigt diese Liste.",
]


def register_handlers(client: discord.Client) -> None:
    @client.event
    async def on_ready():
        start_scheduler(client)

    @client.event
    async def on_message(message: discord.Message):
        if message.author == client.user:
            return
        if message.channel.id not in settings.ALLOWED_CHANNEL_IDS:
            return

        raw = message.content.strip()
        content = raw.lower()

        # Rewrite hidden aliases to their canonical form (only for detection)
        for hidden, maps_to in HIDDEN_ALIASES.items():
            if content.startswith(hidden):
                content = content.replace(hidden, maps_to, 1)
                break

        # Determine which command we’re handling
        trigger = content.split()[0] if content else ""
        cmd_key = CANONICAL.get(trigger)
        if not cmd_key:
            return  # ignore other messages

        # Ensure we have the latest schedule
        load_schedule_if_changed()

        # Route to handlers
        if cmd_key == "help":
            await _handle_help(message)
        elif cmd_key == "today":
            await _handle_today(message)
        elif cmd_key == "day":
            await _handle_day(message, raw)
        elif cmd_key == "all":
            await _handle_all(message)
        elif cmd_key == "now":
            await _handle_now(message)
        elif cmd_key == "next":
            await _handle_next(message)
        elif cmd_key == "step":
            await _handle_step(message, raw)


# -------- Handlers --------

async def _handle_help(message: discord.Message):
    # Compose help with visible pairs; %naechster is intentionally not shown.
    lines = [
        "%today / %heute — gibt alle für heute geplanten Schritte aus.",
        "%day <d> / %tag <d> — zeigt alle Schritte für Tag d.",
        "%all / %alle — zeigt alle geplanten Schritte (Zeit + Titel) für den gesamten Zyklus.",
        "%now / %jetzt — zeigt den zuletzt fälligen Schritt von heute.",
        "%next / %nächster — zeigt den nächsten Schritt.",
        "%step <d> <n> / %schritt <d> <n> — zeigt den n-ten Schritt von Tag d (voller Text).",
        "%help / %hilfe — zeigt diese Liste.",
    ]
    await message.channel.send("\n".join(f"• {l}" for l in lines))


async def _handle_today(message: discord.Message):
    now = datetime.now(TZ)
    daynum = cycle_day_for_public(now.date())
    evs = get_events_for_day(daynum)
    if not evs:
        await message.channel.send(f"**Heute (Tag {daynum}):** keine Schritte geplant.")
        return

    lines = []
    for i, ev in enumerate(evs, start=1):
        lines.append(_fmt_time_and_optional_title(ev, idx=i))
    await message.channel.send(f"**Heute (Tag {daynum}):**\n" + "\n".join(lines))


async def _handle_day(message: discord.Message, raw: str):
    parts = raw.split()
    if len(parts) != 2:
        await message.channel.send("Benutzung: `%day <d>` / `%tag <d>`")
        return
    try:
        d = int(parts[1])
    except ValueError:
        await message.channel.send("Bitte eine Zahl für den Tag angeben, z. B. `%tag 3`.")
        return

    evs = get_events_for_day(d)
    if not evs:
        await message.channel.send(f"**Tag {d}:** keine Schritte geplant.")
        return

    lines = []
    for i, ev in enumerate(evs, start=1):
        lines.append(_fmt_time_and_optional_title(ev, idx=i))
    await message.channel.send(f"**Tag {d}:**\n" + "\n".join(lines))


async def _handle_all(message: discord.Message):
    # We want all days with time + (optional) title
    from qi_bot.schedule.loader import schedule_data as _sd  # local import to avoid cycles
    days_dict = _sd.get("days", {})
    if not days_dict:
        await message.channel.send("Keine Daten verfügbar.")
        return

    chunks = []
    for d, evs, day_title in _all_days_iter(_sd):
        if not evs:
            continue
        header = f"**Tag {d}**" + (f" — {day_title}" if day_title else "")
        lines = [ _fmt_time_and_optional_title(ev) for ev in evs ]
        chunks.append(header + "\n" + "\n".join(lines))

    await message.channel.send("\n\n".join(chunks) if chunks else "Keine Daten verfügbar.")


async def _handle_now(message: discord.Message):
    now = datetime.now(TZ)
    latest, nxt, daynum = _find_now_and_next_for_today(now)

    if latest:
        # Send FULL message for the latest (no title header)
        await send_full_now(message.channel, latest)
    else:
        if nxt:
            hhmm = nxt.get("time", "??:??")
            await message.channel.send(f"Heute noch nichts fällig. Nächster Schritt (Tag {daynum}): **{hhmm} Uhr**.")
        else:
            # No events today at all → find next day with content
            d2, first = _find_first_event_after_today(now)
            if d2 and first:
                await message.channel.send(f"Heute keine Schritte. Nächster Tag mit Inhalt: **Tag {d2}**, {first.get('time','??:??')} Uhr.")
            else:
                await message.channel.send("Keine weiteren Schritte gefunden.")


async def _handle_next(message: discord.Message):
    now = datetime.now(TZ)
    _, nxt, daynum = _find_now_and_next_for_today(now)
    if nxt:
        # Send FULL message for the next (no title header)
        await send_full_now(message.channel, nxt)
        return
    # else: scan future days
    d2, first = _find_first_event_after_today(now)
    if d2 and first:
        await send_full_now(message.channel, first)
    else:
        await message.channel.send("Keine weiteren Schritte gefunden.")


async def _handle_step(message: discord.Message, raw: str):
    parts = raw.split()
    if len(parts) != 3:
        await message.channel.send("Benutzung: `%step <d> <n>` / `%schritt <d> <n>`")
        return
    try:
        d = int(parts[1]); n = int(parts[2])
    except ValueError:
        await message.channel.send("Bitte Zahlen für Tag und Index angeben, z. B. `%schritt 2 1`.")
        return
    evs = get_events_for_day(d)
    if not evs:
        await message.channel.send(f"**Tag {d}:** keine Schritte geplant.")
        return
    if not (1 <= n <= len(evs)):
        await message.channel.send(f"Tag {d} hat **{len(evs)}** Schritte. Index **{n}** ist ungültig.")
        return
    await send_full_now(message.channel, evs[n - 1])
