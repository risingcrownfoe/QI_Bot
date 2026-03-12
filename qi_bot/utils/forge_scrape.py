from __future__ import annotations
from typing import Any
import logging
import requests

log = logging.getLogger("qi-bot")

API_URL = "https://api.forge-db.com/api/datatables/players/de/de14?draw=1&columns[0][data]=rank&columns[0][name]=&columns[0][searchable]=true&columns[0][orderable]=true&columns[0][search][value]=&columns[0][search][regex]=false&columns[1][data]=avatar&columns[1][name]=&columns[1][searchable]=true&columns[1][orderable]=false&columns[1][search][value]=&columns[1][search][regex]=false&columns[2][data]=name&columns[2][name]=&columns[2][searchable]=true&columns[2][orderable]=true&columns[2][search][value]=&columns[2][search][regex]=false&columns[3][data]=guild_name&columns[3][name]=&columns[3][searchable]=true&columns[3][orderable]=true&columns[3][search][value]=&columns[3][search][regex]=false&columns[4][data]=points&columns[4][name]=&columns[4][searchable]=true&columns[4][orderable]=true&columns[4][search][value]=&columns[4][search][regex]=false&columns[5][data]=points_change&columns[5][name]=&columns[5][searchable]=true&columns[5][orderable]=true&columns[5][search][value]=&columns[5][search][regex]=false&columns[6][data]=battles&columns[6][name]=&columns[6][searchable]=true&columns[6][orderable]=true&columns[6][search][value]=&columns[6][search][regex]=false&columns[7][data]=battles_change&columns[7][name]=&columns[7][searchable]=true&columns[7][orderable]=true&columns[7][search][value]=&columns[7][search][regex]=false&columns[8][data]=guild_id&columns[8][name]=&columns[8][searchable]=true&columns[8][orderable]=true&columns[8][search][value]=&columns[8][search][regex]=false&columns[9][data]=player_id&columns[9][name]=&columns[9][searchable]=true&columns[9][orderable]=true&columns[9][search][value]=&columns[9][search][regex]=false&columns[10][data]=is_inactive&columns[10][name]=&columns[10][searchable]=true&columns[10][orderable]=true&columns[10][search][value]=&columns[10][search][regex]=false&order[0][column]=0&order[0][dir]=asc&order[0][name]=&start=0&length=-1"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
    "Referer": "https://foestats.com/",
    "Origin": "https://foestats.com",
    "Connection": "keep-alive",
}


def fetch_players(timeout: int = 60) -> list[dict[str, Any]]:
    log.info("[forge] Fetching players from %s", API_URL)

    with requests.Session() as session:
        session.headers.update(HEADERS)
        r = session.get(API_URL, timeout=timeout)

    if r.status_code != 200:
        log.error("[forge] status=%s", r.status_code)
        log.error("[forge] response headers=%s", dict(r.headers))
        log.error("[forge] response body snippet=%r", r.text[:1000])
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
        player_id = _coerce_int(p.get("player_id") or p.get("playerId"), default=0)
        if player_id <= 0:
            continue

        guild_name = str(p.get("guild_name")).strip()
        guild_id_raw = p.get("guild_id") or p.get("guildId") or 0
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
