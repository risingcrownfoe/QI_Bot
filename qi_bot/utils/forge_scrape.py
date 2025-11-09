# qi_bot/utils/forge_scrape.py
import csv
import io
import requests
import datetime
from zoneinfo import ZoneInfo

API_URL = "https://api.dev.forge-db.com/api/datatables/players/de/de14?draw=1&start=0&length=-1"

def fetch_players(timeout=60):
    r = requests.get(API_URL, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    # Some datatables endpoints wrap rows in "data"
    rows = payload.get("data", payload)
    if not isinstance(rows, list):
        raise ValueError("Unexpected API response format: 'data' is not a list")
    return rows

def build_daily_csv_text(rows, min_battles=10_000, min_points=5_000_000):
    """
    Returns CSV text with columns:
    name,player_id,guild_name,guild_id,points,battles
    """
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["name", "player_id", "guild_name", "guild_id", "points", "battles"])

    kept = 0
    for p in rows:
        # Be defensive: use .get with defaults and normalize types
        name       = p.get("name", "")
        player_id  = p.get("player_id", p.get("playerId", ""))  # handle either key
        guild_name = p.get("guild_name", "")
        guild_id   = p.get("guild_id", "")
        points     = p.get("points", 0)
        battles    = p.get("battles", 0)

        # Some fields may arrive as strings; normalize
        try:
            player_id = int(player_id)
        except Exception:
            pass
        try:
            points = int(points)
        except Exception:
            pass
        try:
            battles = int(battles)
        except Exception:
            # keep original if not numeric
            pass

        # apply thresholds
        if battles < min_battles or points < min_points:
            continue

        w.writerow([name, player_id, guild_name, guild_id, points, battles])
        kept += 1

    return buf.getvalue()

def make_daily_filename(prefix="daily_data"):
    # Use UTC for consistency; switch to local tz if you prefer
    now = datetime.datetime.now(ZoneInfo("Europe/Zurich"))
    ts = now.strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.csv"
