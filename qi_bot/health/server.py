import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

from qi_bot.config import settings

log = logging.getLogger("qi-bot")

def start_health_server():
    port = settings.PORT

    class Handler(BaseHTTPRequestHandler):
        def _ok_headers(self):
            self.send_response(200)
            self.send_header("Content-type", "text/plain; charset=utf-8")
            self.end_headers()

        def _classify(self):
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            ua = self.headers.get("User-Agent", "")
            xff = self.headers.get("X-Forwarded-For", "")
            cip = self.client_address[0]
            is_self = (
                self.headers.get("X-QI-Self-Ping") == "1"
                or qs.get("sp", ["0"])[0] == "1"
                or "qi-bot-self-ping/1" in ua
            )
            is_uptime = "uptimerobot" in ua.lower() or "uptime-robot" in ua.lower()
            origin = "self" if is_self else "ext"
            hint = "uptimerobot" if (not is_self and is_uptime) else ""
            ctx = {"ua": ua, "xff": xff, "ip": cip, "path": parsed.path, "query": parsed.query}
            return origin, hint, ctx

        def do_GET(self):
            origin, hint, ctx = self._classify()
            self._ok_headers()
            try:
                self.wfile.write(b"ok")
            except Exception:
                pass
            hint_tag = f"[{hint}]" if hint else ""
            log.info(
                "[http][%s][GET]%s path=%s ip=%s xff=%s ua=%s",
                origin,
                hint_tag,
                ctx["path"] + (f"?{ctx['query']}" if ctx["query"] else ""),
                ctx["ip"],
                ctx["xff"],
                ctx["ua"],
            )

        def do_HEAD(self):
            origin, hint, ctx = self._classify()
            self._ok_headers()
            hint_tag = f"[{hint}]" if hint else ""
            log.info(
                "[http][%s][HEAD]%s path=%s ip=%s xff=%s ua=%s",
                origin,
                hint_tag,
                ctx["path"] + (f"?{ctx['query']}" if ctx["query"] else ""),
                ctx["ip"],
                ctx["xff"],
                ctx["ua"],
            )

        def log_message(self, format, *args):
            return

    server = HTTPServer(("0.0.0.0", port), Handler)
    log.info("[health] Listening on 0.0.0.0:%s", port)
    server.serve_forever()
