import sys
import time
import asyncio
import logging
import threading
import psycopg2
import config as cfg
from engine import CopyTrader, run_health_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def keep_neon_alive():
    conn = None
    while True:
        try:
            if conn is None or conn.closed:
                conn = psycopg2.connect(cfg.DATABASE_URL, sslmode="require")
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            logging.info("🟢 Neon keep-alive ping sent")
        except Exception as e:
            logging.warning(f"Neon keep-alive failed: {e}")
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
        time.sleep(180)  # every 3 minutes

async def main():
    logging.info("⚡ Booting Polymarket CopyBot Core Runtime Pipeline Interface...")

    # 1. Start web interface dashboard container thread for infrastructure uptime validation
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    # 2. Start Neon keep-alive thread
    neon_thread = threading.Thread(target=keep_neon_alive, daemon=True)
    neon_thread.start()

    # 3. Initialize bot — CopyTrader.__init__ already calls fetch_with_retry and
    # sets cfg.compounding_bankroll / cfg.peak_bankroll, so no second fetch is
    # needed here. A duplicate call would make two live RPC round-trips and
    # silently overwrite the values set inside __init__ (fix A).
    bot = CopyTrader(dry_run=cfg.DRY_RUN)
    cfg._bot_ref = bot  # Connect tracking state pointer back globally to metric servers

    # 4. Start WebSocket listener task so live trade signals are actually received.
    # The listener is created in CopyTrader.__init__ but never scheduled — without
    # this create_task the entire WS signal path (_on_ws_signal, instant copies,
    # live price updates) is permanently dead (fix B).
    if bot._ws_listener is not None:
        asyncio.create_task(bot._ws_listener.run())
        logging.info("⚡ WebSocket market channel listener task started")
    else:
        logging.warning("WebSocket listener not available — running on REST polling only")

    # User channel: delivers unambiguous order-level signals per tracked wallet.
    # Started alongside the market channel; the engine handles both via the same
    # _on_ws_event callback, distinguishing them by ev["kind"] == "user_trade".
    if bot._user_listener is not None:
        asyncio.create_task(bot._user_listener.run())
        logging.info("⚡ WebSocket user channel listener task started")

    # 5. Fall into continuous automated execution polling loop
    while True:
        try:
            await bot.scan_and_copy()
        except (
            # Transient network / IO failures — safe to retry on the next poll.
            OSError,
            asyncio.TimeoutError,
        ) as transient_err:
            logging.warning(
                f"Transient error in scan loop — will retry in {cfg.POLL_INTERVAL}s: "
                f"{transient_err}"
            )
        except Exception:
            # Anything else (KeyError, TypeError, AttributeError, logic bugs …)
            # is unexpected. Log the full traceback so it's diagnosable, then
            # re-raise so the process exits rather than spinning in a broken state.
            logging.critical("Fatal error in scan loop — shutting down.", exc_info=True)
            raise

        await asyncio.sleep(cfg.POLL_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🛑 Program gracefully stopped by operator command signal interrupt.")
