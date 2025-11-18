# qi_bot/api/foe.py
from __future__ import annotations

import logging
from typing import Any, Dict, List

from qi_bot.utils.cloudfare_d1 import d1_query

log = logging.getLogger("qi-bot")


def fetch_snapshots() -> List[Dict[str, Any]]:
    """Return all snapshots as a list of dicts.

    Each item: { "id": int, "label": str, "captured_at": str }
    """
    sql = """
        SELECT id, label, captured_at
        FROM snapshots
        ORDER BY captured_at DESC;
    """
    res = d1_query(sql)

    # D1: result is a list of statement results, we only send 1 statement.
    statements = res.get("result") or []
    if not statements:
        return []

    rows = statements[0].get("results") or []
    # rows are already dict-like: {"id": ..., "label": ..., "captured_at": ...}
    return rows


def fetch_players_for_snapshot(snapshot_id: int) -> List[Dict[str, Any]]:
    """Return all player rows for the given snapshot.

    Each item: { "player_id", "guild_id", "era_nr", "points", "battles" }
    """
    sql = """
        SELECT player_id, guild_id, era_nr, points, battles
        FROM player_stats
        WHERE snapshot_id = ?;
    """
    res = d1_query(sql, [snapshot_id])

    statements = res.get("result") or []
    if not statements:
        return []

    rows = statements[0].get("results") or []
    return rows
