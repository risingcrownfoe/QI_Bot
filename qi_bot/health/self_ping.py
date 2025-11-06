import logging
import threading
import time as pytime
import urllib.request
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import os

log = logging.getLogger("qi-bot")

def _resolve_base_url() -> str | None:
    # Preference: HEALTH_URL (env) -> RENDER_EXTERNAL_URL (injected by Render)
    base = os.getenv("HEALTH_URL") or os.getenv("RENDER_EXTERNAL_URL")
    if not base:
        return None
    base = base.strip()
    if not base.startswith("http"):
        base = "https://" + base
    return base

def start_self_ping():
    base = _resolve_base_url()
    if not base:
        log.warning("[self-ping] disabled (no HEALTH_URL/RENDER_EXTERNAL_URL)")
        return

    parts = list(urlparse(base))
    if not parts[2].endswith("/"):
        parts[2] += "/"

    q = parse_qs(parts[4], keep_blank_values=True)
    q["sp"] = ["1"]
    parts[4] = urlencode(q, doseq=True)
    url = urlunparse(parts)

    def loop():
        log.info("[self-ping] target: %s", url)
        while True:
            t0 = pytime.time()
            try:
                req = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": "qi-bot-self-ping/1",
                        "X-QI-Self-Ping": "1",
                    },
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    dt_ms = int((pytime.time() - t0) * 1000)
                    log.info("[self-ping] %s in %dms", resp.status, dt_ms)
            except Exception as e:
                dt_ms = int((pytime.time() - t0) * 1000)
                log.error("[self-ping] ERROR after %dms | %s", dt_ms, e)
            pytime.sleep(60)

    threading.Thread(target=loop, daemon=True).start()
