import logging
from datetime import datetime

import discord

from qi_bot.config import settings
from qi_bot.scheduler.loop import (
    start_scheduler,
    send_preview,
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

        content = message.content.strip().lower()

        if content == "%today":
            load_schedule_if_changed()
            now = datetime.now(TZ)
            day = cycle_day_for_public(now.date())
            evs = get_events_for_day(day)
            if not evs:
                await message.channel.send(f"**Today: Day {day}** — no messages scheduled.")
                return
            times = "\n".join(f"• {e['time']} Uhr" for e in evs)
            await message.channel.send(f"**Today: Day {day}**\n__Scheduled:__\n{times}")
            return

        if content.startswith("%preview"):
            parts = message.content.strip().split()
            if len(parts) != 3:
                await message.channel.send("Usage: `%preview <day> <n>`")
                return
            try:
                d, n = int(parts[1]), int(parts[2])
            except ValueError:
                await message.channel.send("Numbers only.")
                return
            load_schedule_if_changed()
            evs = get_events_for_day(d)
            if not evs or not (1 <= n <= len(evs)):
                await message.channel.send(f"Day {d} has {len(evs)} events.")
                return
            await message.channel.send(f"Sending Day {d} – event #{n} ({evs[n-1]['time']})…")
            await send_preview(message.channel, evs[n - 1])
            return

        if content == "%alldays":
            load_schedule_if_changed()
            days = client  # dummy to keep type-checkers quiet in f-string below
            _ = days  # not used; just to avoid a lint warning

            # pull schedule from loader again to avoid importing internals
            from qi_bot.schedule.loader import schedule_data as _sd  # local import to avoid cycles
            days_dict = _sd.get("days", {})
            lines = []
            for d in sorted(days_dict, key=lambda x: int(x)):
                evs = days_dict[d]
                if isinstance(evs, dict):
                    ev_list = evs.get("events", [])
                else:
                    ev_list = evs
                if ev_list:
                    times = "\n".join(f"• {e['time']} Uhr" for e in ev_list)
                    lines.append(f"**Day {d}:**\n{times}")
            await message.channel.send("\n\n".join(lines) or "No schedule data.")
            return
