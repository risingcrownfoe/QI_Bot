import threading

from qi_bot.logging_setup import setup_logging
from qi_bot.config import settings
from qi_bot.health.server import start_health_server
from qi_bot.health.self_ping import start_self_ping

from qi_bot.bot.client import create_client
from qi_bot.bot.commands import register_handlers

log = setup_logging()

def main():
    # Start health server + self-ping
    threading.Thread(target=start_health_server, daemon=True).start()
    start_self_ping()

    # Discord client
    client = create_client()
    register_handlers(client)

    log.info("[init] starting Discord client")
    client.run(settings.DISCORD_TOKEN)

if __name__ == "__main__":
    main()
