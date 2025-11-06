import json
from pathlib import Path
from typing import Any, Dict, List

from qi_bot.config import settings
from qi_bot.utils.jsonx import strip_comments_and_trailing_commas

# In-memory schedule state (shared)
schedule_data: Dict[str, Any] = {"days": {}, "templates": {}}
_schedule_mtime: float | None = None

def load_schedule_if_changed(force: bool = False) -> None:
    """(Re)load messages.json if it changed on disk."""
    global schedule_data, _schedule_mtime
    p = Path(settings.SCHEDULE_FILE)
    if not p.exists():
        schedule_data = {"days": {}, "templates": {}}
        _schedule_mtime = None
        return
    mtime = p.stat().st_mtime
    if force or _schedule_mtime != mtime:
        raw = p.read_text(encoding="utf-8")
        cleaned = strip_comments_and_trailing_commas(raw)
        data = json.loads(cleaned)
        data.setdefault("days", {})
        data.setdefault("templates", {})
        schedule_data = data
        _schedule_mtime = mtime

def get_schedule_data() -> Dict[str, Any]:
    return schedule_data

def get_events_for_day(day_number: int) -> List[Dict[str, Any]]:
    days = schedule_data.get("days", {})
    events = days.get(str(day_number), [])
    # Back-compat: if day structure is an object with "events"
    if isinstance(events, dict):
        events = events.get("events", [])
    return sorted(events, key=lambda e: e["time"])
