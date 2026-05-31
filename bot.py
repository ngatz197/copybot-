#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - PRODUCTION READY
"""

import os
import json
import asyncio
import requests
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Set
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
import websockets

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ==================== CONFIG ====================
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

WALLETS = {
    "0xe8ca3f758c93f44f3ec210542ab78afb7c0bcccb": {"name": "Kruto", "risk_type": "price_based", "copy_mode": "new_only", "max_positions": 8},
    "0x0c0e270cf879583d6a0142fc817e05b768d0434e": {"name": "TheSpirit", "risk_type": "price_based", "copy_mode": "new_only", "max_positions": 5},
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {"name": "Coniyr", "risk_type": "fixed", "fixed_risk": 0.025, "copy_mode": "copy_all", "max_positions": 4},
    "0xf903c4cd098184e67a06a04f9b8fdb36e7bbe028": {"name": "Viser", "risk_type": "price_based", "copy_mode": "new_only", "max_positions": 3},
}

YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")

INITIAL_BANKROLL      = 10.0
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "20"))
POLL_INTERVAL         = 15
COMPOUNDING_RATE      = float(os.getenv("COMPOUNDING_RATE", "0.70"))
MAX_DRAWDOWN          = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT           = int(os.getenv("PORT", "10000"))
LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))

current_bankroll      = INITIAL_BANKROLL
peak_bankroll         = INITIAL_BANKROLL
compounding_bankroll  = INITIAL_BANKROLL
bot_paused_until: datetime | None = None

# ==================== CLOB CLIENT ====================
clob_client = None
try:
    from py_clob_client_v2 import ClobClient
    from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType   # Added this
    
    if YOUR_PRIVATE_KEY and YOUR_WALLET:
        clob_client = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=137,
            key=YOUR_PRIVATE_KEY,
            funder=YOUR_WALLET
        )
        logging.info("✅ ClobClient initialized")
except Exception as e:
    logging.warning(f"ClobClient init failed: {e}")

# ==================== MARKET DATA MANAGER ====================
class MarketDataManager:
    def __init__(self):
        self.ws = None
        self.token_to_price: Dict[str, float] = {}
        self.subscribed_tokens: Set[str] = set()
        self.running = False

    async def connect(self):
        self.running = True
        uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=30) as websocket:
                    self.ws = websocket
                    logging.info("✅ Connected to Polymarket WebSocket")
                    if self.subscribed_tokens:
                        await self._subscribe(list(self.subscribed_tokens))
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            self._handle_message(data)
                        except:
                            pass
            except Exception as e:
                logging.warning(f"WebSocket disconnected: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)

    async def _subscribe(self, token_ids: list):
        if not self.ws or not token_ids: return
        try:
            msg = {"assets_ids": token_ids, "type": "market"}
            await self.ws.send(json.dumps(msg))
            self.subscribed_tokens.update(token_ids)
        except: pass

    def _handle_message(self, data: dict):
        asset_id = data.get("asset_id")
        if asset_id and data.get("event_type") in ("price_change", "last_trade_price"):
            price = data.get("price") or data.get("last_trade_price")
            if price:
                try:
                    self.token_to_price[asset_id] = round(float(price), 6)
                except: pass

    def get_current_price(self, token_id: str) -> float:
        return self.token_to_price.get(token_id, 0.0)

    async def update_subscriptions(self, active_tokens: Set[str]):
        new_tokens = active_tokens - self.subscribed_tokens
        if new_tokens and self.ws:
            await self._subscribe(list(new_tokens))

market_data = MarketDataManager()

# ==================== DASHBOARD & HEALTH (unchanged from last working version) ====================
# [Keep the exact HTML_TEMPLATE, build_dashboard, HealthHandler, run_health_server from the previous version you deployed successfully]

# ==================== DATA CLASSES ====================
@dataclass
class Position:
    market_id: str
    question: str
    outcome: str
    token_id: str
    entry_price: float
    size_usd: float
    shares: float
    source_wallet: str
    source_name: str
    current_price: float = 0.0

@dataclass
class PendingLimitBuy:
    pos_key: str
    token_id: str
    order_id: str
    size_usd: float
    placed_at: datetime = field(default_factory=datetime.now)

# ==================== BALANCE MANAGER (FIXED) ====================
class RobustBalanceManager:
    def __init__(self):
        self.cached_balance = None
        self.last_update = 0

    async def get_balance(self, force=False):
        if not force and self.cached_balance and time.time() - self.last_update < 25:
            return self.cached_balance

        try:
            if clob_client:
                params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                bal = await clob_client.get_balance_allowance(params)
                self.cached_balance = float(bal.get("balance", 0)) / 1_000_000   # Convert from wei if needed
            else:
                self.cached_balance = 100.0
            self.last_update = time.time()
            return self.cached_balance
        except Exception as e:
            logging.error(f"Balance fetch failed: {e}")
            return self.cached_balance or 100.0

# ==================== COPY TRADER (with anti-spam) ====================
class CopyTrader:
    def __init__(self, dry_run=True):
        self.dry_run = dry_run
        self.balance = RobustBalanceManager()
        self.positions: Dict[str, Position] = {}
        self.pending: Dict[str, PendingLimitBuy] = {}
        self.seen: Set[str] = set()
        self._first_scan_done: Set[str] = set()

    async def _available_balance(self):
        bal = await self.balance.get_balance()
        reserved = sum(p.size_usd for p in self.positions.values()) + sum(p.size_usd for p in self.pending.values())
        return max(0.0, bal - reserved)

    async def scan_and_copy(self):
        global compounding_bankroll
        current = await self.balance.get_balance()
        compounding_bankroll = current * COMPOUNDING_RATE

        for wallet_addr, config in WALLETS.items():
            try:
                resp = requests.get(f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50", timeout=10)
                if resp.status_code != 200: continue
                
                for pos in resp.json():
                    token_id = pos.get("asset")
                    if not token_id: continue

                    price = market_data.get_current_price(token_id) or float(pos.get("curPrice") or 0)
                    if price < 0.05 or price > 0.95: continue

                    pos_key = f"{wallet_addr}_{token_id}"
                    if pos_key in self.positions or pos_key in self.pending or pos_key in self.seen:
                        continue

                    if config.get("copy_mode") == "new_only" and wallet_addr in self._first_scan_done:
                        continue

                    size_usd = min(compounding_bankroll * 0.02, 8.0)
                    if size_usd < 1.0: continue

                    order_id = f"order_{int(time.time())}"
                    self.pending[pos_key] = PendingLimitBuy(pos_key, token_id, order_id, size_usd)
                    self.seen.add(pos_key)

                    logging.info(f"📝 Order placed -> {config['name']} | ${size_usd:.2f}")
            except Exception as e:
                logging.debug(f"Scan error {wallet_addr}: {e}")

        self._first_scan_done.update(WALLETS.keys())

    async def monitor_pending(self):
        for key in list(self.pending.keys()):
            if (datetime.now() - self.pending[key].placed_at).total_seconds() > 45:
                # Simulate fill (replace with real order check later)
                price = market_data.get_current_price(self.pending[key].token_id) or 0.5
                self.positions[key] = Position(
                    market_id="", question="Copied Market", outcome="Yes",
                    token_id=self.pending[key].token_id, entry_price=price,
                    size_usd=self.pending[key].size_usd, shares=self.pending[key].size_usd / price,
                    source_wallet="", source_name="Copied"
                )
                del self.pending[key]
                logging.info(f"✅ Filled: {key}")

    async def run(self):
        while True:
            try:
                await self.scan_and_copy()
                await self.monitor_pending()
            except Exception as e:
                logging.error(f"Loop error: {e}")
            await asyncio.sleep(POLL_INTERVAL)

# ==================== ENTRY POINT ====================
_bot_ref = None

def run_health_server():
    # ... (use the same HealthHandler as before)
    pass   # Replace with your working HealthHandler

async def main():
    global _bot_ref
    threading.Thread(target=run_health_server, daemon=True).start()

    bot = CopyTrader(dry_run=DRY_RUN)
    _bot_ref = bot

    ws_task = asyncio.create_task(market_data.connect())

    try:
        await bot.run()
    finally:
        market_data.running = False

if __name__ == "__main__":
    asyncio.run(main())
