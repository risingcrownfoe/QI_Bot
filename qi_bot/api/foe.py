# qi_bot/api/foe.py
from __future__ import annotations
from datetime import date

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
        - recruitment_status             (from player_recruitment.status, if any)
        - recruitment_note               (from player_recruitment.note, if any)
        - recruitment_last_contacted_at  (ISO date string, if any)
    """
    sql = """
        SELECT
            ps.player_id,
            pn.player_name AS player_name,
            ps.guild_id,
            gn.guild_name AS guild_name,
            ps.era_nr,
            ps.points,
            ps.battles,
            pr.status AS recruitment_status,
            pr.note AS recruitment_note,
            pr.last_contacted_at AS recruitment_last_contacted_at
        FROM player_stats AS ps
        LEFT JOIN player_names       AS pn ON ps.player_id = pn.player_id
        LEFT JOIN guild_names        AS gn ON ps.guild_id = gn.guild_id
        LEFT JOIN player_recruitment AS pr ON ps.player_id = pr.player_id
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

def update_player_recruitment(
    player_id: int,
    recruitment_status: str | None,
    recruitment_note: str | None,
    recruitment_last_contacted_at: str | None,
) -> Dict[str, Any]:
    """Create or update recruitment info for a player.

    - player_id: numeric FoE player id
    - recruitment_status: e.g. 'ignored', 'declined', 'fresh'
    - recruitment_note: optional free-text note
    - recruitment_last_contacted_at: ISO date string (YYYY-MM-DD); if missing, defaults to today
    """

    # Basic validation / normalization
    status = (recruitment_status or "").strip()
    if not status:
        raise ValueError("recruitment_status is required")

    note = (recruitment_note or "").strip()
    last = (recruitment_last_contacted_at or "").strip()
    if not last:
        # fallback to today if frontend didn’t send anything
        last = date.today().isoformat()

    # If you want to be stricter, you can enforce allowed statuses:
    # allowed = {"ignored", "declined", "fresh"}
    # if status not in allowed:
    #     raise ValueError(f"Invalid status '{status}', must be one of {sorted(allowed)}")

    sql = """
        INSERT INTO player_recruitment (player_id, status, note, last_contacted_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(player_id) DO UPDATE SET
          status = excluded.status,
          note = excluded.note,
          last_contacted_at = excluded.last_contacted_at;
    """

    d1_query(sql, [player_id, status, note, last])

    # Return a JSON shape that matches what the frontend expects / uses
    return {
        "player_id": player_id,
        "recruitment_status": status,
        "recruitment_note": note,
        "recruitment_last_contacted_at": last,
    }
