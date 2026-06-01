import sys
import asyncio
import logging
import threading
import config as cfg
from services import CopyTrader, run_health_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

async def main():
    logging.info("⚡ Booting Polymarket CopyBot Core Runtime Pipeline Interface...")

    # 1. Start web interface dashboard container thread for infrastructure uptime validation
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    # 2. Initialize application tracking instance service engine layers
    bot = CopyTrader(dry_run=cfg.DRY_RUN)
    cfg._bot_ref = bot  # Connect tracking state pointer back globally to metric servers

    # 3. Secure initial blockchain network collateral state synchronization
    try:
        starting_balance = bot.balance.fetch_with_retry(retries=5, delay=10)
        cfg.peak_bankroll = starting_balance
        cfg.compounding_bankroll = starting_balance
        logging.info(f"✅ Balance verification synchronized successfully. Capital base: ${starting_balance:.2f} pUSD")
    except Exception as e:
        logging.critical(f"❌ Critical initialization failure mapping RPC balance parameters: {e}")
        sys.exit(1)

    # 4. Fall into continuous automated execution polling loop
    while True:
        try:
            await bot.scan_and_copy()
        except Exception as loop_err:
            logging.error(f"⚠️ Internal iteration anomaly caught on loop check: {loop_err}")
        
        await asyncio.sleep(cfg.POLL_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🛑 Program gracefully stopped by operator command signal interrupt.")