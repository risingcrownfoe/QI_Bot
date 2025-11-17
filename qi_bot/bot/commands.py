# qi_bot/bot/commands.py

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

log = logging.getLogger("qi-bot")

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

TZ = ZoneInfo(settings.TIMEZONE)

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
        "hidden": ["%naechster"]  # accepted, not shown in help
    },
    "step": {
        "en": ["%step"],
        "de": ["%schritt"],
        "hidden": ["%s"]
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

        content = message.content.strip()
        if not content.startswith("%"):
            return

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

        # Ensure we have the latest schedule
        load_schedule_if_changed()

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


# -------- Help builders (English/German menus split) --------

def _build_help_english() -> str:
    """
    English-only commands for easy copy in Discord (shown as inline code spans).
    """
    rows = [
        ("%today",      "gives all steps scheduled for today."),
        ("%day d",      "shows all steps for day d.\n"
                        "    e.g. `%day 1` shows all steps for **Thursday**, the first day of QI."),
        ("%step d n",   "shows the n-th step for day d.\n"
                        "    e.g. `%step 1 2` shows the second message of day 1."),
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
        ("%schritt t n","zeigt den n-ten Schritt für Tag t.\n"
                        "    z. B. `%schritt 1 2` zeigt die zweite Nachricht vom ersten Tag."),
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
    today = datetime.now(TZ).date()
    await _handle_day(message, f"%day {cycle_day_for_public(today)}", "de", "%day")


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
    evs = get_events_for_day(d)
    if not evs:
        if lang == "de":
            await message.channel.send(f"**Tag {d}:** keine Schritte geplant.")
        else:
            await message.channel.send(f"**Day {d}:** no steps scheduled.")
        return
    date_for_day = datetime.now(TZ).date()
    await message.channel.send(
        f"**Tag {d} ({date_for_day.isoformat()}):** {len(evs)} Schritte geplant."
    )
    for ev in evs:
        await send_full_now(message.channel, ev)


async def _handle_all(message: discord.Message):
    lines = []
    for step in range(settings.CYCLE_LENGTH):
        d = step + 1
        evs = get_events_for_day(d)
        if not evs:
            continue
        lines.append(f"**Tag {d}:** {len(evs)} Schritte geplant.")
    if not lines:
        await message.channel.send("Keine geplanten Schritte gefunden.")
    else:
        await message.channel.send("\n".join(lines))


def _find_now_and_next_for_today(now_dt):
    daynum = cycle_day_for_public(now_dt.date())
    evs = get_events_for_day(daynum)
    latest = None
    nxt = None
    for ev in evs:
        t = ev.get("time")
        if not t:
            continue
        try:
            hh, mm = map(int, t.split(":"))
        except Exception:
            continue
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


async def _handle_now(message: discord.Message):
    now = datetime.now(TZ)
    latest, _, daynum = _find_now_and_next_for_today(now)
    if latest:
        await send_full_now(message.channel, latest)
    else:
        await message.channel.send(
            f"**Tag {daynum}:** kein bereits gesendeter Schritt gefunden."
        )


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


async def _handle_step(message: discord.Message, raw: str, lang: str, used_alias: str):
    parts = raw.split()
    if len(parts) != 3:
        await message.channel.send(_usage_step(lang))
        return
    try:
        d = int(parts[1]); n = int(parts[2])
    except ValueError:
        await message.channel.send(_usage_step(lang))
        return
    evs = get_events_for_day(d)
    if not evs:
        await message.channel.send(f"**Tag {d}:** keine Schritte geplant.")
        return
    if not (1 <= n <= len(evs)):
        await message.channel.send(
            f"Tag {d} hat **{len(evs)}** Schritte. Index **{n}** ist ungültig."
        )
        return
    await send_full_now(message.channel, evs[n - 1])


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
            "Benutzung: `%schritt t n`\n"
            "    z. B. `%schritt 1 2` zeigt die zweite Nachricht vom ersten Tag."
        )
    else:
        return (
            "Usage: `%step d n`\n"
            "    e.g. `%step 1 2` shows the second message of day 1."
        )
