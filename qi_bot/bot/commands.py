# qi_bot/bot/commands.py

import logging
from datetime import datetime, timedelta

import discord

from qi_bot.config import settings, get_plan_for_channel
from qi_bot.scheduler.loop import (
    start_scheduler,
    send_full_now,
    cycle_day_for_public,
    run_manual_snapshot_public,
)

from qi_bot.schedule.loader import (
    load_schedule_if_changed,
    get_events_for_day,
    get_schedule_data,
)


log = logging.getLogger("qi-bot")

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

TZ = ZoneInfo(settings.TIMEZONE)


def _schedule_file_for_channel_id(channel_id: int) -> str:
    """Return the schedule file to use for a given channel."""
    plan = get_plan_for_channel(channel_id)
    if plan:
        return plan.schedule_file
    # Fallback: default schedule
    return settings.SCHEDULE_FILE


def _schedule_file_for_message(message: discord.Message) -> str:
    return _schedule_file_for_channel_id(message.channel.id)


# --- Half-day helpers (DE only) ---

# Two-letter German day codes → QI day numbers
DAYCODE_TO_DAYNUM = {
    "do": 1,  # Donnerstag
    "fr": 2,  # Freitag
    "sa": 3,  # Samstag
    "so": 4,  # Sonntag
    "mo": 5,  # Montag
    "di": 6,  # Dienstag
    "mi": 7,  # Mittwoch
}

DAYNUM_TO_NAME_DE = {
    1: "Donnerstag",
    2: "Freitag",
    3: "Samstag",
    4: "Sonntag",
    5: "Montag",
    6: "Dienstag",
    7: "Mittwoch",
}

HALF_TOKENS_MORNING = ("früh", "frueh", "morgen")   # with / without umlaut
HALF_TOKENS_EVENING = ("spät", "spaet", "abend")


def _parse_halfday_from_alias(used_alias: str):
    """
    Parse something like '%dofrüh' / '%dofrueh' / '%dospaet' into
    (daynum:int, half:str|'morning'|'evening'|None).

    Returns (None, None) on parse error.
    """
    alias = used_alias.strip().lower()
    if alias.startswith("%"):
        alias = alias[1:]

    if len(alias) < 3:
        return None, None

    day_code = alias[:2]       # 'do', 'fr', 'sa', ...
    half_token = alias[2:]     # 'früh', 'frueh', 'spät', 'spaet', ...

    daynum = DAYCODE_TO_DAYNUM.get(day_code)
    if daynum is None:
        return None, None

    if half_token in HALF_TOKENS_MORNING:
        half = "morning"
    elif half_token in HALF_TOKENS_EVENING:
        half = "evening"
    else:
        half = None

    return daynum, half


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
    """Return 'N. HH:MM Uhr: <title>' or 'HH:MM Uhr:' (no title) according to request."""
    hhmm = ev.get("time", "??:??")
    title = ev.get("title")  # may be None
    prefix = f"{idx}. " if idx is not None else ""
    if title:
        return f"{prefix}{hhmm} Uhr: {title}"
    else:
        return f"{prefix}{hhmm} Uhr:"


def _find_now_and_next_for_today(now_dt, schedule_file: str):
    """Return (latest_ev_or_None, next_ev_or_None, today_daynum) limited to *today only*."""
    today = now_dt.date()
    daynum = cycle_day_for_public(today)
    events = get_events_for_day(daynum, schedule_file=schedule_file)
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


def _find_first_event_after_today(now_dt, schedule_file: str):
    """Find the next day with events after today; scan up to one full cycle."""
    for step in range(1, settings.CYCLE_LENGTH + 1):
        dt = now_dt + timedelta(days=step)
        dnum = cycle_day_for_public(dt.date())
        evs = get_events_for_day(dnum, schedule_file=schedule_file)
        if evs:
            return dnum, evs[0]
    return None, None


def _find_most_recent_event_across_days(now_dt, schedule_file: str):
    """
    Find the most recent event at or before 'now', scanning backwards up to one full cycle.
    """
    best_ev = None
    best_daynum = None
    best_dt = None

    for step in range(0, settings.CYCLE_LENGTH):
        dt_day = now_dt - timedelta(days=step)
        dnum = cycle_day_for_public(dt_day.date())
        evs = get_events_for_day(dnum, schedule_file=schedule_file)
        if not evs:
            continue
        for ev in evs:
            try:
                hh, mm = map(int, ev["time"].split(":"))
            except Exception:
                continue
            ev_dt = dt_day.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if ev_dt <= now_dt and (best_dt is None or ev_dt > best_dt):
                best_ev = ev
                best_dt = ev_dt
                best_daynum = dnum

    return best_ev, best_daynum, best_dt


# Define visible aliases per language and hidden ones here; the router builds maps from this.
COMMAND_ALIASES = {
    "help": {
        "en": ["%help"],
        "de": ["%hilfe"],
        "hidden": []
    },
    "today": {
        "en": ["%today"],
        "de": ["%heute"],
        "hidden": []
    },
    "day": {
        "en": ["%day"],
        "de": ["%tag"],
        "hidden": ["%d"]
    },
    "all": {
        "en": ["%all"],
        "de": ["%alle"],
        "hidden": []
    },
    "now": {
        "en": ["%now"],
        "de": ["%jetzt"],
        "hidden": []
    },
    "next": {
        "en": ["%next"],
        "de": ["%nächster"],
        "hidden": ["%naechster", "%n"]  # accepted, not shown in help
    },
    "step": {
        "en": ["%step"],
        "de": ["%schritt"],
        "hidden": ["%s"]
    },
    "sql": {
        "en": ["%sql"],
        "de": ["%sql"],
        "hidden": []
    },
    "halfday": {
        "en": [],  # no EN commands for now
        "de": [
            "%dofrüh", "%dospät",
            "%frfrüh", "%frspät",
            "%safrüh", "%saspät",
            "%sofrüh", "%sospät",
            "%mofrüh", "%mospät",
            "%difrüh", "%dispät",
            "%mifrüh", "%mispät",
        ],
        # ASCII fallbacks without umlauts (hidden in help)
        "hidden": [
            "%dofrueh", "%dospaet",
            "%frfrueh", "%frspaet",
            "%safrueh", "%saspaet",
            "%sofrueh", "%sospaet",
            "%mofrueh", "%mospaet",
            "%difrueh", "%dispaet",
            "%mifrueh", "%mispaet",
        ],
    },
}

# Build a fast lookup: alias -> (cmd_key, lang, is_hidden)
ALIAS_LOOKUP = {}
for key, spec in COMMAND_ALIASES.items():
    for a in spec.get("en", []):
        ALIAS_LOOKUP[a.lower()] = (key, "en", False)
    for a in spec.get("de", []):
        ALIAS_LOOKUP[a.lower()] = (key, "de", False)
    for a in spec.get("hidden", []):
        # hidden currently only used for DE; adjust if you add EN hidden aliases later
        ALIAS_LOOKUP[a.lower()] = (key, "de", True)


# -------- Register handlers --------

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
        if not raw:
            return
        content = raw.lower()

        # Determine the trigger token (first word)
        trigger = content.split()[0]

        # If it looks like a command but not recognized, give a friendly hint
        if trigger.startswith("%") and trigger not in ALIAS_LOOKUP:
            await message.channel.send(
                "Unbekannter Befehl. Probiere `%hilfe` (Deutsch) oder `%help` (English)."
            )
            return

        # Map to canonical command and language
        entry = ALIAS_LOOKUP.get(trigger)
        if not entry:
            return  # not a command for us

        cmd_key, lang, _is_hidden = entry
        raw = content

        # Ensure we have the latest schedule for this channel's plan
        schedule_file = _schedule_file_for_message(message)
        load_schedule_if_changed(schedule_file=schedule_file)

        # Route to handlers, passing lang + used alias where useful
        if cmd_key == "help":
            await _handle_help(message, lang)
        elif cmd_key == "today":
            await _handle_today(message)
        elif cmd_key == "day":
            await _handle_day(message, raw, lang, trigger)
        elif cmd_key == "all":
            await _handle_all(message)
        elif cmd_key == "now":
            await _handle_now(message)
        elif cmd_key == "next":
            await _handle_next(message)
        elif cmd_key == "step":
            await _handle_step(message, raw, lang, trigger)
        elif cmd_key == "sql":
            await _handle_sql(message)
        elif cmd_key == "halfday":
            await _handle_half_day(message, lang, trigger)


# -------- Help builders (English/German menus split) --------

def _build_help_english() -> str:
    """
    English-only commands for easy copy in Discord (shown as inline code spans).
    """
    rows = [
        ("%today",      "gives all steps scheduled for today."),
        ("%day d",      "shows all steps for day d.\n"
                        "    e.g. `%day 1` shows all steps for **Thursday**, the first day of QI."),
        ("%step n",     "shows the n-th step across all days.\n"
                        "    e.g. `%step 5` shows the fifth scheduled message overall."),
        ("%now",        "shows the most recent step (across days)."),
        ("%next",       "shows the next step."),
        ("%all",        "shows all scheduled steps for the entire QI."),
        ("%help",       "shows this menu in English."),
        ("%hilfe",      "zeigt das Hilfemenü auf Deutsch."),
    ]
    lines = ["**Available commands (English):**"]
    for cmd, desc in rows:
        lines.append(f"`{cmd}` – {desc}")
    return "\n".join(lines)


def _build_help_german() -> str:
    rows = [
        ("%heute",      "zeigt alle Schritte, die für heute geplant sind."),
        ("%tag t",      "zeigt alle Schritte für Tag t.\n"
                        "    z. B. `%tag 1` zeigt alle Schritte für **Donnerstag**, den ersten Tag der QI."),
        ("%schritt n",  "zeigt den n-ten Schritt über alle Tage hinweg.\n"
                        "    z. B. `%schritt 5` zeigt die fünfte Nachricht insgesamt."),
        ("%jetzt",      "zeigt den zuletzt gesendeten Schritt (über alle Tage)."),
        ("%nächster",   "zeigt den nächsten geplanten Schritt."),
        ("%alle",       "zeigt alle geplanten Schritte für die gesamte QI."),
        ("%help",       "shows the help menu in English."),
        ("%hilfe",      "zeigt dieses Hilfemenü auf Deutsch."),
    ]
    lines = ["**Verfügbare Befehle (Deutsch):**"]
    for cmd, desc in rows:
        lines.append(f"`{cmd}` – {desc}")
    return "\n".join(lines)


# -------- Command handlers --------

async def _handle_help(message: discord.Message, lang: str):
    if lang == "de":
        txt = _build_help_german()
    else:
        txt = _build_help_english()
    await message.channel.send(txt)


async def _handle_today(message: discord.Message):
    now = datetime.now(TZ)
    schedule_file = _schedule_file_for_message(message)
    daynum = cycle_day_for_public(now.date())
    evs = get_events_for_day(daynum, schedule_file=schedule_file)
    if not evs:
        await message.channel.send(f"**Heute (Tag {daynum}):** keine Schritte geplant.")
        return

    lines = []
    for i, ev in enumerate(evs, start=1):
        lines.append(_fmt_time_and_optional_title(ev, idx=i))
    await message.channel.send(f"**Heute (Tag {daynum}):**\n" + "\n".join(lines))

async def _handle_day(message: discord.Message, raw: str, lang: str, used_alias: str):
    parts = raw.split()
    if len(parts) != 2:
        await message.channel.send(_usage_day(lang))
        return
    try:
        d = int(parts[1])
    except ValueError:
        await message.channel.send(_usage_day(lang))
        return

    schedule_file = _schedule_file_for_message(message)
    evs = get_events_for_day(d, schedule_file=schedule_file)
    if not evs:
        await message.channel.send(f"**Tag {d}:** keine Schritte geplant.")
        return

    lines = []
    for i, ev in enumerate(evs, start=1):
        lines.append(_fmt_time_and_optional_title(ev, idx=i))
    await message.channel.send(f"**Tag {d}:**\n" + "\n".join(lines))

async def _handle_all(message: discord.Message):
    from qi_bot.schedule.loader import get_schedule_data as _get_sd  # local import to avoid cycles

    schedule_file = _schedule_file_for_message(message)
    _sd = _get_sd(schedule_file)

    days_dict = _sd.get("days", {})
    if not days_dict:
        await message.channel.send("Keine Daten verfügbar.")
        return

    chunks = []
    for d, evs, day_title in _all_days_iter(_sd):
        if not evs:
            continue
        header = f"**Tag {d}**" + (f" — {day_title}" if day_title else "")
        lines = [_fmt_time_and_optional_title(ev) for ev in evs]
        chunks.append(header + "\n" + "\n".join(lines))

    await message.channel.send("\n\n".join(chunks) if chunks else "Keine Daten verfügbar.")




async def _handle_now(message: discord.Message):
    now = datetime.now(TZ)
    schedule_file = _schedule_file_for_message(message)

    # NEW: search backwards across days (up to CYCLE_LENGTH) for the most recent event
    most_recent, mr_daynum, mr_dt = _find_most_recent_event_across_days(now, schedule_file)

    if most_recent:
        await send_full_now(message.channel, most_recent)
        return

    # Fallbacks if nothing found in the past window (unlikely if schedule is populated)
    _, nxt, daynum = _find_now_and_next_for_today(now, schedule_file)
    if nxt:
        hhmm = nxt.get("time", "??:??")
        await message.channel.send(f"Heute noch nichts fällig. Nächster Schritt (Tag {daynum}): **{hhmm} Uhr**.")
        return

    d2, first = _find_first_event_after_today(now, schedule_file)
    if d2 and first:
        await message.channel.send(f"Heute keine Schritte. Nächster Tag mit Inhalt: **Tag {d2}**, {first.get('time','??:??')} Uhr.")
    else:
        await message.channel.send("Keine weiteren Schritte gefunden.")


async def _handle_next(message: discord.Message):
    now = datetime.now(TZ)
    schedule_file = _schedule_file_for_message(message)

    _, nxt, daynum = _find_now_and_next_for_today(now, schedule_file)
    if nxt:
        # Send FULL message for the next (no title header)
        await send_full_now(message.channel, nxt)
        return
    # else: scan future days
    d2, first = _find_first_event_after_today(now, schedule_file)
    if d2 and first:
        await send_full_now(message.channel, first)
    else:
        await message.channel.send("Keine weiteren Schritte gefunden.")


async def _handle_step(message: discord.Message, raw: str, lang: str, used_alias: str):
    # Expect exactly one numeric argument: the global step index n
    parts = raw.split()
    if len(parts) != 2:
        await message.channel.send(_usage_step(lang))
        return

    try:
        n = int(parts[1])
    except ValueError:
        await message.channel.send(_usage_step(lang))
        return

    if n <= 0:
        await message.channel.send(_usage_step(lang))
        return

    # Load full schedule and flatten all events across all days in order
    from qi_bot.schedule.loader import get_schedule_data as _get_sd  # local import to avoid cycles
    schedule_file = _schedule_file_for_message(message)
    _sd = _get_sd(schedule_file)

    all_events = []
    for d, evs, day_title in _all_days_iter(_sd):
        for ev in evs:
            all_events.append(ev)

    if not all_events:
        if lang == "de":
            await message.channel.send("Keine Schritte geplant.")
        else:
            await message.channel.send("No steps scheduled.")
        return

    total = len(all_events)
    if n > total:
        if lang == "de":
            await message.channel.send(
                f"Es gibt insgesamt **{total}** Schritte. Index **{n}** ist ungültig."
            )
        else:
            await message.channel.send(
                f"There are **{total}** steps in total. Index **{n}** is out of range."
            )
        return

    # n is 1-based index into the flattened list
    ev = all_events[n - 1]
    await send_full_now(message.channel, ev)

async def _handle_half_day(message: discord.Message, lang: str, used_alias: str):
    """
    Handle commands like %dofrüh, %dospät, %frfrüh, %frspät, ...
    All of them are routed here via the 'halfday' command key.
    """
    daynum, half = _parse_halfday_from_alias(used_alias)

    # Should not happen if aliases & parser are in sync, but be defensive.
    if daynum is None or half is None:
        if lang == "de":
            await message.channel.send(
                "Konnte aus dem Befehl den Tag / Halbtag nicht erkennen. "
                "Beispiele: `%dofrüh`, `%dospät`, `%frfrüh`, `%frspät`."
            )
        else:
            await message.channel.send(
                "Could not parse day / half-day from command."
            )
        return

    schedule_file = _schedule_file_for_message(message)
    evs = get_events_for_day(daynum, schedule_file=schedule_file)
    if not evs:
        if lang == "de":
            await message.channel.send(f"**Tag {daynum}:** keine Schritte geplant.")
        else:
            await message.channel.send(f"**Day {daynum}:** no steps scheduled.")
        return

    # Decide which events belong to the half-day
    def _is_in_half(ev):
        t = ev.get("time")
        if not t:
            return False
        try:
            hh, mm = map(int, t.split(":"))
        except Exception:
            return False

        # Convention: 'früh' = strictly before 12:00,
        # 'spät' = 12:00 and later
        if half == "morning":
            return hh < 12
        else:  # "evening"
            return hh >= 12

    selected = [ev for ev in evs if _is_in_half(ev)]

    if not selected:
        if lang == "de":
            part_label = "Vormittag" if half == "morning" else "Nachmittag/Abend"
            await message.channel.send(
                f"**Tag {daynum} ({part_label})**: keine Schritte in diesem Zeitraum."
            )
        else:
            part_label = "morning" if half == "morning" else "afternoon/evening"
            await message.channel.send(
                f"**Day {daynum} ({part_label})**: no steps in this time window."
            )
        return

    weekday_de = DAYNUM_TO_NAME_DE.get(daynum, f"Tag {daynum}")

    if lang == "de":
        part_label = "Morgen" if half == "morning" else "Abend"
        header = f"**Tag {daynum} – {weekday_de} {part_label}:**"
    else:
        part_label = "morning" if half == "morning" else "afternoon/evening"
        header = f"**Day {daynum} – {part_label}:**"

    # One short header + all full messages for that half-day in order
    await message.channel.send(header)
    for ev in selected:
        await send_full_now(message.channel, ev)




async def _handle_sql(message: discord.Message):
    """Manually trigger a FoE → D1 snapshot (%sql)."""
    # Optionally, you could send a 'starting...' message here.
    await run_manual_snapshot_public(message.channel)


# -------- Usage messages (lang-specific, no angle brackets) --------

def _usage_day(lang: str) -> str:
    if lang == "de":
        return (
            "Benutzung: `%tag t`\n"
            "    z. B. `%tag 1` zeigt alle Schritte für **Donnerstag**, den ersten Tag der QI."
        )
    else:
        return (
            "Usage: `%day d`\n"
            "    e.g. `%day 1` shows all steps for **Thursday**, the first day of QI."
        )


def _usage_step(lang: str) -> str:
    if lang == "de":
        return (
            "Benutzung: `%schritt n`\n"
            "    z. B. `%schritt 5` zeigt die fünfte Nachricht über alle Tage hinweg."
        )
    else:
        return (
            "Usage: `%step n`\n"
            "    e.g. `%step 5` shows the fifth message across all days."
        )

