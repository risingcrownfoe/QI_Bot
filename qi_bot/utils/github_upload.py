# qi_bot/utils/github_upload.py
import os, base64, json, requests, datetime

OWNER    = os.environ.get("GITHUB_OWNER", "risingcrownfoe")
REPO     = os.environ.get("GITHUB_REPO", "QI_Bot")
BRANCH   = os.environ.get("GITHUB_BRANCH", "main")
DATA_DIR = os.environ.get("GITHUB_DATA_DIR", "data")
TOKEN    = os.environ["GITHUB_TOKEN"]  # must be set in Render

def push_time_csv():
    """
    Builds a CSV (hour,minute,second) with current UTC time and uploads it
    to GitHub as data/YYYYMMDD_HHMMSS.csv. Returns dict with html_url, etc.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    ts = now.strftime("%Y%m%d_%H%M%S")
    hour, minute, second = now.strftime("%H"), now.strftime("%M"), now.strftime("%S")

    csv_text = f"hour,minute,second\n{hour},{minute},{second}\n"
    path = f"{DATA_DIR}/{ts}.csv"
    url  = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{path}"
    payload = {
        "message": f"Add {path}",
        "content": base64.b64encode(csv_text.encode("utf-8")).decode("ascii"),
        "branch": BRANCH,
    }

    r = requests.put(
        url,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        data=json.dumps(payload),
        timeout=30,
    )
    r.raise_for_status()
    j = r.json()
    return {
        "path": j["content"]["path"],
        "html_url": j["content"]["html_url"],
        "commit_url": j["commit"]["html_url"],
    }

def push_csv_under_data(filename: str, csv_text: str):
    """
    Uploads given CSV text to GitHub at data/<filename> on BRANCH.
    Returns dict with html_url and commit_url.
    """
    path = f"{DATA_DIR}/{filename}"
    url  = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{path}"
    payload = {
        "message": f"Add {path}",
        "content": base64.b64encode(csv_text.encode("utf-8")).decode("ascii"),
        "branch": BRANCH,
    }

    r = requests.put(
        url,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        data=json.dumps(payload),
        timeout=60,
    )
    try:
        r.raise_for_status()
    except requests.HTTPError as err:
        raise RuntimeError(f"GitHub API error {r.status_code}: {r.text}") from err

    j = r.json()
    return {
        "path": j["content"]["path"],
        "html_url": j["content"]["html_url"],
        "commit_url": j["commit"]["html_url"],
    }
