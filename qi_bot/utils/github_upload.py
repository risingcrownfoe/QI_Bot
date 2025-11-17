# qi_bot/utils/github_upload.py
# NOTE: repurposed to push CSV data into Cloudflare D1 via a Worker,
#       instead of uploading to GitHub.

import os
import json
import requests

# URL of the Cloudflare Worker endpoint, e.g.:
#   https://foe-data-import.<your-account>.workers.dev/import-daily
CF_IMPORT_URL = os.environ["CF_IMPORT_URL"]

# Shared secret that must match the IMPORT_SECRET in wrangler.toml
CF_IMPORT_SECRET = os.environ["CF_IMPORT_SECRET"]


def push_csv_under_data(filename: str, csv_text: str):
    """
    Send the daily CSV to the Cloudflare Worker that writes into D1.

    Arguments:
        filename: e.g. "daily_data_20251117_040031.csv"
        csv_text: CSV string starting with header:
                  name,player_id,guild_name,guild_id,points_change,battles_change,era,points,battles

    Returns:
        Parsed JSON response from the Worker, e.g.:
        {
          "ok": true,
          "filename": "...",
          "label": "...",
          "captured_at": "...",
          "snapshot_id": 3,
          "inserted": 1234,
          "html_url": "Imported into D1 snapshot 3 (1234 rows)"
        }
    """
    payload = {"filename": filename, "csv_text": csv_text}

    r = requests.post(
        CF_IMPORT_URL,
        headers={
            "Content-Type": "application/json",
            "X-Import-Secret": CF_IMPORT_SECRET,
        },
        data=json.dumps(payload),
        timeout=60,
    )
    try:
        r.raise_for_status()
    except requests.HTTPError as err:
        raise RuntimeError(
            f"Cloudflare import error {r.status_code}: {r.text}"
        ) from err

    return r.json()
