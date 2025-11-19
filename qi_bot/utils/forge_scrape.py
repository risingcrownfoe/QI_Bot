# qi_bot/utils/forge_scrape.py
"""Utilities for fetching and pre-filtering FoE player data.

This module is now *CSV-free*: it only fetches raw rows from the Forge-DB
API and turns them into a list of simple dicts that are ready to push into
our Cloudflare D1 database.
"""

from __future__ import annotations
from typing import Any, Dict, List
import requests
import logging

from qi_bot.utils.foe_eras import era_nr_from_str

log = logging.getLogger("qi-bot")

API_URL = (
    "https://api.dev.forge-db.com/api/datatables/players/de/de14?draw=1&start=0&length=-1"
)

# Same order as used in the offline converter / SQLite import

def fetch_players(timeout: int = 60) -> list[dict[str, Any]]:
    """Fetch all player rows from Forge-DB.

    Returns the raw JSON "rows" as a list of dicts.
    """
    log.info("[forge] Fetching players from %s", API_URL)
    r = requests.get(API_URL, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    rows = payload.get("data", payload)
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected payload shape from Forge-DB: {type(rows)!r}")
    log.info("[forge] Got %d raw rows", len(rows))
    return rows


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _extract_era_nr(player: Dict[str, Any]) -> int:
    raw = player.get("raw") or {}
    era_str = str(raw.get("era", "")).strip()
    return era_nr_from_str(era_str)


def build_daily_rows(
    rows: List[Dict[str, Any]],
    min_battles: int = 10_000,
    min_points: int = 5_000_000,
) -> List[Dict[str, int]]:
    """Turn raw Forge-DB rows into compact dicts for D1.

    We keep only:
        - player_id
        - guild_id
        - era_nr
        - points
        - battles

    and we apply the same thresholds as before:
        - battles >= min_battles
        - points  >= min_points
    """
    out: List[Dict[str, int]] = []
    kept = 0

    for p in rows:
        # The API uses either "player_id" or "playerId" depending on version;
        # support both just in case.
        player_name = str(p.get("name")).strip()
        player_id = _coerce_int(p.get("player_id"), default=0)
        if player_id <= 0:
            continue

        guild_name = str(p.get("guild_name")).strip()
        guild_id_raw = p.get("guild_id") or 0
        guild_id = _coerce_int(guild_id_raw, default=0)

        points = _coerce_int(p.get("points"), default=0)
        battles = _coerce_int(p.get("battles"), default=0)

        if battles < min_battles or points < min_points:
            continue

        era_nr = _extract_era_nr(p)

        out.append(
            {
                "player_id": player_id,
                "guild_id": guild_id,
                "era_nr": era_nr,
                "points": points,
                "battles": battles,
                "player_name": player_name,
                "guild_name": guild_name,
            }
        )
        kept += 1

    log.info(
        "[forge] Filtered %d/%d rows (battles>=%d & points>=%d)",
        kept,
        len(rows),
        min_battles,
        min_points,
    )
    return out
