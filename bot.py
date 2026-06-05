"""
bot.py — Polymarket Copy Trading Bot entry point
No Telegram. Starts the monitor loop and prints status to stdout/logs.
"""

import asyncio
import logging
import signal
import sys

import config
import services
from services import state

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def handle_shutdown(sig, frame):
    logger.info("Signal %s received — shutting down…", sig)
    state.running = False


async def status_printer() -> None:
    """Print a status summary every 5 minutes."""
    while state.running:
        await asyncio.sleep(300)
        if state.running:
            logger.info("\n%s", services.get_status_text())


async def main() -> None:
    # Validate config
    if not config.MY_WALLET_ADDRESS:
        logger.error("MY_WALLET_ADDRESS is not set. Exiting.")
        sys.exit(1)

    if not config.MY_WALLET_PRIVATE_KEY and not config.DRY_RUN:
        logger.error("MY_WALLET_PRIVATE_KEY is not set and DRY_RUN=false. Exiting.")
        sys.exit(1)

    mode = "DRY RUN" if config.DRY_RUN else "*** LIVE TRADING ***"
    logger.info("=" * 60)
    logger.info("Polymarket Copy Bot starting — %s", mode)
    logger.info("Watching %d wallets:", len(config.SOURCE_WALLETS))
    for lbl, addr in config.SOURCE_WALLETS.items():
        logger.info("  %-8s %s", lbl, addr)
    logger.info("Trade size : 1%% of wallet balance ($%.0f–$%.0f guardrails)",
                config.MIN_ORDER_USDC, config.MAX_ORDER_USDC)
    logger.info("Order type : GTC limit  |  tick offset: %.4f  |  TTL: %ds",
                config.LIMIT_TICK_OFFSET, config.LIMIT_ORDER_TTL_SEC)
    logger.info("Poll       : every %ds", config.POLL_INTERVAL_SEC)
    logger.info("=" * 60)

    state.running = True

    # Register SIGTERM/SIGINT for graceful Render shutdown
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT,  handle_shutdown)

    await asyncio.gather(
        services.monitor_loop(),
        status_printer(),
    )

    logger.info("Bot stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
