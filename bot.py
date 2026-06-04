import sys
import time
import asyncio
import logging
import threading
import psycopg2
from concurrent.futures import ThreadPoolExecutor
import config as cfg
from engine import CopyTrader, run_health_server
from exchange import PolymarketUserChannelListener

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

db_executor = ThreadPoolExecutor(max_workers=3)

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
        time.sleep(180)

def handle_task_exception(task: asyncio.Task):
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logging.critical(f"💥 Background task worker loop failed: {task.get_name()} -> {e}", exc_info=True)


async def execution_queue_consumer(queue: asyncio.Queue, bot_engine: CopyTrader):
    """
    PolyGun Event-Driven Consumer Hub.
    Bypasses polling timers entirely to run trades concurrently the microsecond they appear.
    """
    logging.info("🚀 PolyGun High-Speed Execution Queue Consumer is active and monitoring...")
    while True:
        ev = await queue.get()
        try:
            wallet = ev.get("proxyWallet", "").lower()
            token_id = ev.get("asset")
            side = ev.get("side") # "BUY" or "SELL"
            price = float(ev.get("price", 0.0))
            size = float(ev.get("size", 0.0))

            # Fetch wallet profile directly from matrix definitions
            wallet_cfg = cfg.WALLETS.get(wallet)
            if not wallet_cfg:
                continue

            logging.info(f"⚡ [EVENT-MATCH] Intercepted Whale Action from {wallet_cfg['name']}: {side} {token_id} @ {price}")

            if side == "BUY":
                # Compute fractional copy allocations instantly
                usd_allocation = bot_engine.balance_manager.get_available_balance() * cfg.COMPOUNDING_RATE
                
                logging.info(f"🛒 Processing BUY event (Mode: {wallet_cfg['copy_mode']}) for {token_id}")
                await bot_engine.executor.create_and_sign_limit_buy(
                    token_id=token_id, 
                    price=price, 
                    size_usd=usd_allocation
                )
            elif side == "SELL":
                # Match positions and close or trim matching token units instantly
                await bot_engine.executor.execute_limit_sell(
                    token_id=token_id, 
                    shares=size, 
                    price=price 
                )

        except Exception as worker_err:
            logging.error(f"Error executing queued trade frame: {worker_err}", exc_info=True)
        finally:
            queue.task_done()


async def main():
    logging.info("⚡ Booting PolyGun-Optimized Polymarket Pipeline Infrastructure Interface...")

    # 1. Threaded Health / UI Layer Container Boot
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    # 2. Database Session Maintenance Container Boot
    neon_thread = threading.Thread(target=keep_neon_alive, daemon=True)
    neon_thread.start()

    # 3. Instantiate Core Engine Configurations
    bot = CopyTrader()
    cfg._bot_ref = bot

    # 4. Instantiate Lock-Free Asynchronous Priority Processing Queue
    shared_execution_queue = asyncio.Queue()

    # 5. Spin Up Event Consumer Worker Pool
    consumer_task = asyncio.create_task(
        execution_queue_consumer(shared_execution_queue, bot), 
        name="ExecutionConsumer"
    )
    consumer_task.add_done_callback(handle_task_exception)

    # 6. Initialize listener explicitly configured to clear out the old URL 
    high_speed_user_ws = PolymarketUserChannelListener(
        wallet_addrs=list(cfg.WALLETS.keys()), 
        queue=shared_execution_queue
    )

    ws_user_task = asyncio.create_task(high_speed_user_ws.run(), name="HighSpeedUserWS")
    ws_user_task.add_done_callback(handle_task_exception)

    # 7. Passive Reconciliation Engine Poller 
    while True:
        try:
            await bot.scan_and_copy()
        except (OSError, asyncio.TimeoutError) as transient_err:
            logging.warning(f"Transient polling error in validation handler: {transient_err}")
        except Exception:
            logging.critical("Fatal breakdown in validation engine polling layer.", exc_info=True)
            raise

        await asyncio.sleep(cfg.POLL_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🛑 Operations gracefully suspended via hardware interrupt signal.")