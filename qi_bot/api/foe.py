# qi_bot/api/foe.py
from __future__ import annotations

import logging
from typing import Any, Dict, List

from qi_bot.utils.cloudfare_d1 import d1_query
from qi_bot.utils.foe_eras import era_str_from_nr

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

    Each item includes:
        - player_id
        - player_name (if known)
        - guild_id
        - guild_name (if known)
        - era_nr
        - era   (string, e.g. "IronAge")
        - points
        - battles
    """
    sql = """
        SELECT
            ps.player_id,
            pn.player_name AS player_name,
            ps.guild_id,
            gn.guild_name AS guild_name,
            ps.era_nr,
            ps.points,
            ps.battles
        FROM player_stats AS ps
        LEFT JOIN player_names AS pn ON ps.player_id = pn.player_id
        LEFT JOIN guild_names AS gn ON ps.guild_id = gn.guild_id
        WHERE ps.snapshot_id = ?;
    """

    res = d1_query(sql, [snapshot_id])

    statements = res.get("result") or []
    if not statements:
        return []

    rows = statements[0].get("results") or []

    # Attach human-readable era string
    for row in rows:
        era_nr = row.get("era_nr")
        try:
            era_nr_int = int(era_nr) if era_nr is not None else 0
        except Exception:
            era_nr_int = 0

        row["era"] = era_str_from_nr(era_nr_int) or "Unknown"

    return rows
