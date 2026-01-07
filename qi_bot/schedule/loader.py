# qi_bot/schedule/loader.py

import json
from pathlib import Path
from typing import Any, Dict, List

from qi_bot.config import settings
from qi_bot.utils.jsonx import strip_comments_and_trailing_commas

# Backwards-compatible "default" schedule (for settings.SCHEDULE_FILE)
schedule_data: Dict[str, Any] = {"days": {}, "templates": {}}

# New: per-file caches
_schedule_cache: Dict[str, Dict[str, Any]] = {}
_schedule_mtimes: Dict[str, float] = {}


def _load_single_schedule(path: Path, force: bool = False) -> Dict[str, Any]:
    """Load a single schedule file, with caching by mtime."""
    key = str(path.resolve())

    if not path.exists():
        data: Dict[str, Any] = {"days": {}, "templates": {}}
        _schedule_cache[key] = data
        _schedule_mtimes[key] = 0.0

        # Keep old global name in sync for the default file
        if path.name == settings.SCHEDULE_FILE:
            # mutate existing global dict instead of rebinding
            schedule_data.clear()
            schedule_data.update(data)

        return data

    mtime = path.stat().st_mtime
    prev_mtime = _schedule_mtimes.get(key)

    if not force and prev_mtime == mtime and key in _schedule_cache:
        return _schedule_cache[key]

    raw = path.read_text(encoding="utf-8")
    data = json.loads(strip_comments_and_trailing_commas(raw))
    data.setdefault("days", {})
    data.setdefault("templates", {})

    _schedule_cache[key] = data
    _schedule_mtimes[key] = mtime

    # Keep "schedule_data" for legacy callers (default file)
    if path.name == settings.SCHEDULE_FILE:
        # mutate existing global dict instead of rebinding
        schedule_data.clear()
        schedule_data.update(data)

    return data


def load_schedule_if_changed(
    force: bool = False,
    schedule_file: str | None = None,
) -> None:
    """(Re)load schedule_file if it changed on disk.

    If no file is given, uses settings.SCHEDULE_FILE (backwards compatible).
    """
    if schedule_file is None:
        schedule_file = settings.SCHEDULE_FILE
    _load_single_schedule(Path(schedule_file), force=force)


def get_schedule_data(schedule_file: str | None = None) -> Dict[str, Any]:
    """Return the parsed schedule data for a given file (or default)."""
    if schedule_file is None:
        schedule_file = settings.SCHEDULE_FILE
    path = Path(schedule_file)
    key = str(path.resolve())

    if key not in _schedule_cache:
        _load_single_schedule(path, force=True)

    return _schedule_cache[key]


def get_events_for_day(
    day_number: int,
    schedule_file: str | None = None,
) -> List[Dict[str, Any]]:
    """Return the list of events for a given day from a given schedule file."""
    data = get_schedule_data(schedule_file)
    days = data.get("days", {})
    events = days.get(str(day_number), [])

    # Back-compat: if day structure is an object with "events"
    if isinstance(events, dict):
        events = events.get("events", [])

    return sorted(events, key=lambda e: e["time"])
