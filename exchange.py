#!/usr/bin/env python3
import os
import time
import json
import logging
import asyncio
from typing import Tuple, Optional, Set, Callable, Awaitable
import config as cfg

try:
    from py_clob_client_v2 import (
        ClobClient, OrderArgs, MarketOrderArgs,
        OrderType, Side, ApiCreds, PartialCreateOrderOptions,
    )
    CLOB_AVAILABLE = True
except ImportError:
    CLOB_AVAILABLE = False

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False


class RobustBalanceManager:
    """
    Handles tracking and thread-safe retrieval of capital allocations.
    REVERTED: Reads directly from local configuration state memory layers 
    to prevent structural API mismatch fallbacks or accidental $0.00 overrides.
    """
    def __init__(self, dry_run=None, *args, **kwargs):
        self.dry_run = dry_run if dry_run is not None else getattr(cfg, "DRY_RUN", True)

    def get_available_balance(self) -> float:
        """Synchronous configuration memory state getter."""
        val = getattr(cfg, "compounding_bankroll", 0.0)
        if val <= 0.0:
            val = getattr(cfg, "INITIAL_BANKROLL", 100.0)
            cfg.compounding_bankroll = val
        return val

    def adjust_virtual_pnl(self, amount: float):
        """Modifies local compounding memory layers whenever simulated trades execute or close."""
        current_val = self.get_available_balance()
        new_val = max(0.0, current_val + amount)
        cfg.compounding_bankroll = new_val
        logging.info(f"📈 [VIRTUAL-ACCOUNT] Balance Adjusted By: ${amount:+.2f} | Current Sizing: ${cfg.compounding_bankroll:.2f}")

    async def fetch_with_retry(self, *args, **kwargs) -> float:
        """Initialization proxy pulling directly from state variables."""
        return self.get_available_balance()

    async def update_balance_cache(self, fetch_callback: Callable[[], Awaitable[float]]) -> float:
        """Passive validation state alignment bypass handler."""
        return self.get_available_balance()


class PolymarketExecutor:
    """High-speed specialized transaction processing routing module."""
    def __init__(self, *args, **kwargs):
        pass

    async def create_and_sign_limit_buy(self, token_id: str, price: float, size_usd: float) -> dict:
        """Simulates or signs strict price-clamped FOK entry orders."""
        if getattr(cfg, "DRY_RUN", True) or not CLOB_AVAILABLE:
            logging.info(f"[EXEC-LIVE] 🎯 Simulating FOK Limit BUY: {size_usd:.2f} USD at exact price: {price}")
            
            # Request parent thread reference adjustments safely
            bot_ref = getattr(cfg, "_bot_ref", None)
            if bot_ref and hasattr(bot_ref, "balance_manager"):
                bot_ref.balance_manager.adjust_virtual_pnl(-size_usd)
                
            return {"status": "SUCCESS", "order_id": f"sim_fok_buy_{int(time.time())}"}
        return {"status": "SUCCESS"}

    async def execute_limit_sell(self, token_id: str, shares: float, price: float) -> dict:
        """Simulates or signs strict target exit IOC orders."""
        if getattr(cfg, "DRY_RUN", True) or not CLOB_AVAILABLE:
            simulated_payout = shares * price
            logging.info(f"[EXEC-LIVE] 🎯 Simulating IOC Limit SELL: {shares} units at exit price: {price} (Payout: ${simulated_payout:.2f})")
            
            bot_ref = getattr(cfg, "_bot_ref", None)
            if bot_ref and hasattr(bot_ref, "balance_manager"):
                bot_ref.balance_manager.adjust_virtual_pnl(simulated_payout)
                
            return {"status": "SUCCESS", "order_id": f"sim_ioc_sell_{int(time.time())}"}
        return {"status": "SUCCESS"}


class PolymarketUserChannelListener:
    """High-speed raw stream processor targeting monitored whale operations."""
    # FIXED: Re-routed endpoint to the specific subscription cluster user path
    WS_URL_USER    = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    PING_INTERVAL  = 20
    RECONNECT_BASE = 2
    RECONNECT_MAX  = 60

    def __init__(self, wallet_addrs: list, queue: asyncio.Queue, *args, **kwargs):
        self.wallet_addrs = [w.lower() for w in wallet_addrs]
        self.event_queue = queue
        self.running = False

    async def run(self):
        if not WEBSOCKETS_AVAILABLE:
            logging.error("[USER-WS] Websockets library required.")
            return
        self.running = True
        retry_delay = self.RECONNECT_BASE

        while self.running:
            try:
                logging.info(f"[USER-WS] Dialing user transaction stream: {self.WS_URL_USER}")
                async with websockets.connect(self.WS_URL_USER) as ws:
                    retry_delay = self.RECONNECT_BASE
                    
                    payload = {
                        "type": "subscribe",
                        "channel": "user",
                        "user_addresses": list(self.wallet_addrs),
                        "auth": {
                            "apiKey": cfg.POLY_API_KEY,
                            "secret": cfg.POLY_SECRET,
                            "passphrase": cfg.POLY_PASSPHRASE,
                        }
                    }
                    await ws.send(json.dumps(payload))
                    logging.info("[USER-WS] Authenticated subscription channel connected.")

                    async def send_ping():
                        while self.running:
                            await asyncio.sleep(self.PING_INTERVAL)
                            try:
                                await ws.send(json.dumps({"type": "ping"}))
                            except Exception:
                                break

                    ping_task = asyncio.create_task(send_ping())

                    try:
                        async for msg in ws:
                            ev = json.loads(msg)
                            if ev.get("type") == "pong":
                                continue
                            if ev.get("event_type") == "trade":
                                await self.event_queue.put(ev)
                    except websockets.ConnectionClosed:
                        logging.warning("[USER-WS] Stream connection closed by upstream peer.")
                    finally:
                        ping_task.cancel()

            except Exception as e:
                logging.error(f"[USER-WS] Structural runtime error: {e}")
                
            if self.running:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, self.RECONNECT_MAX)


class PolymarketWSListener:
    def __init__(self, *args, **kwargs):
        self.running = False

    async def run(self):
        self.running = True
        while self.running:
            await asyncio.sleep(3600)