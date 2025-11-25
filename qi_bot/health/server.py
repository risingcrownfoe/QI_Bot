import logging
import json
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

        def _json_headers(self, status_code: int = 200):
            """Send JSON + CORS headers."""
            self.send_response(status_code)
            self.send_header("Content-type", "application/json; charset=utf-8")
            # CORS: allow frontend on another domain (Netlify) to call this API.
            # If you prefer, replace * with "https://foe.benjamindettling.ch".
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

        def _cors_preflight(self):
            """Send CORS headers for preflight (OPTIONS) requests."""
            self.send_response(200)
            # Allow your frontend origin; "*" is easiest while developing.
            self.send_header("Access-Control-Allow-Origin", "*")
            # Methods your frontend might use
            self.send_header("Access-Control-Allow-Methods", "GET,PUT,OPTIONS")
            # Headers your frontend sends (at least Content-Type)
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
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
            ctx = {
                "ua": ua,
                "xff": xff,
                "ip": cip,
                "path": parsed.path,
                "query": parsed.query,
            }
            return origin, hint, ctx

        def _read_json_body(self):
            """Read and parse JSON request body, or return {} on failure."""
            length_str = self.headers.get("Content-Length", "0")
            try:
                length = int(length_str)
            except ValueError:
                length = 0

            if length <= 0:
                return {}

            raw = self.rfile.read(length)
            try:
                return json.loads(raw.decode("utf-8"))
            except Exception:
                return {}

        def _handle_foe_get(self, path: str):
            """Handle FoE data API routes under /foe/… (GET)."""
            from qi_bot.api.foe import (
                fetch_snapshots,
                fetch_players_for_snapshot,
            )

            # /foe/snapshots
            # /foe/snapshots/<id>/players
            segments = [seg for seg in path.split("/") if seg]

            # ["foe", "snapshots"]
            if len(segments) == 2 and segments[0] == "foe" and segments[1] == "snapshots":
                data = fetch_snapshots()
                self._json_headers(200)
                self.wfile.write(json.dumps(data).encode("utf-8"))
                return

            # ["foe", "snapshots", "<id>", "players"]
            if (
                len(segments) == 4
                and segments[0] == "foe"
                and segments[1] == "snapshots"
                and segments[3] == "players"
            ):
                try:
                    snapshot_id = int(segments[2])
                except ValueError:
                    self._json_headers(400)
                    self.wfile.write(
                        json.dumps({"error": "invalid snapshot id"}).encode("utf-8")
                    )
                    return

                data = fetch_players_for_snapshot(snapshot_id)
                self._json_headers(200)
                self.wfile.write(json.dumps(data).encode("utf-8"))
                return

            # No route matched: JSON 404
            self._json_headers(404)
            self.wfile.write(json.dumps({"error": "not found"}).encode("utf-8"))

        def _handle_foe_put(self, path: str):
            """Handle FoE data API routes under /foe/… (PUT)."""
            from qi_bot.api.foe import update_player_recruitment

            # /foe/players/<id>/recruitment
            segments = [seg for seg in path.split("/") if seg]

            if (
                len(segments) == 4
                and segments[0] == "foe"
                and segments[1] == "players"
                and segments[3] == "recruitment"
            ):
                # Parse player_id from URL
                try:
                    player_id = int(segments[2])
                except ValueError:
                    self._json_headers(400)
                    self.wfile.write(
                        json.dumps({"error": "invalid player id"}).encode("utf-8")
                    )
                    return

                body = self._read_json_body()
                recruitment_status = body.get("recruitment_status")
                recruitment_note = body.get("recruitment_note")
                recruitment_last_contacted_at = body.get(
                    "recruitment_last_contacted_at"
                )

                try:
                    result = update_player_recruitment(
                        player_id=player_id,
                        recruitment_status=recruitment_status,
                        recruitment_note=recruitment_note,
                        recruitment_last_contacted_at=recruitment_last_contacted_at,
                    )
                except ValueError as e:
                    self._json_headers(400)
                    self.wfile.write(
                        json.dumps({"error": str(e)}).encode("utf-8")
                    )
                    return
                except Exception as e:
                    log.exception("[http][foe][PUT] error updating recruitment: %s", e)
                    self._json_headers(500)
                    self.wfile.write(
                        json.dumps(
                            {"error": "failed to update recruitment info"}
                        ).encode("utf-8")
                    )
                    return

                # Success
                self._json_headers(200)
                self.wfile.write(json.dumps(result).encode("utf-8"))
                return

            # No route matched: JSON 404
            self._json_headers(404)
            self.wfile.write(json.dumps({"error": "not found"}).encode("utf-8"))

        def do_GET(self):
            origin, hint, ctx = self._classify()
            path = ctx["path"]
            hint_tag = f"[{hint}]" if hint else ""

            try:
                # Our FoE JSON API
                if path.startswith("/foe/"):
                    self._handle_foe_get(path)

                # Everything else (including /health) stays as before: plain "ok"
                else:
                    self._ok_headers()
                    try:
                        self.wfile.write(b"ok")
                    except Exception:
                        pass

            except Exception as e:
                # Best-effort error
                log.exception("[http][%s][GET] error for %s: %s", origin, path, e)
                try:
                    # If headers already sent, this will throw, hence try/except.
                    self._json_headers(500)
                    self.wfile.write(
                        json.dumps({"error": "internal error"}).encode("utf-8")
                    )
                except Exception:
                    pass

            # Logging (kept from your existing code)
            log.info(
                "[http][%s][GET]%s path=%s ip=%s xff=%s ua=%s",
                origin,
                hint_tag,
                ctx["path"] + (f"?{ctx['query']}" if ctx["query"] else ""),
                ctx["ip"],
                ctx["xff"],
                ctx["ua"],
            )

        def do_PUT(self):
            origin, hint, ctx = self._classify()
            path = ctx["path"]
            hint_tag = f"[{hint}]" if hint else ""

            try:
                if path.startswith("/foe/"):
                    self._handle_foe_put(path)
                else:
                    # For non-foe paths: 404 JSON
                    self._json_headers(404)
                    self.wfile.write(
                        json.dumps({"error": "not found"}).encode("utf-8")
                    )
            except Exception as e:
                log.exception("[http][%s][PUT] error for %s: %s", origin, path, e)
                try:
                    self._json_headers(500)
                    self.wfile.write(
                        json.dumps({"error": "internal error"}).encode("utf-8")
                    )
                except Exception:
                    pass

            log.info(
                "[http][%s][PUT]%s path=%s ip=%s xff=%s ua=%s",
                origin,
                hint_tag,
                ctx["path"] + (f"?{ctx['query']}" if ctx["query"] else ""),
                ctx["ip"],
                ctx["xff"],
                ctx["ua"],
            )

        def do_OPTIONS(self):
            """Handle CORS preflight."""
            origin, hint, ctx = self._classify()
            path = ctx["path"]
            hint_tag = f"[{hint}]" if hint else ""

            try:
                if path.startswith("/foe/"):
                    self._cors_preflight()
                else:
                    # generic OK for other paths
                    self._cors_preflight()
            except Exception as e:
                log.exception("[http][%s][OPTIONS] error for %s: %s", origin, path, e)

            log.info(
                "[http][%s][OPTIONS]%s path=%s ip=%s xff=%s ua=%s",
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
