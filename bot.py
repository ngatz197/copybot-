#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - PRODUCTION READY (CLOB V2 + WebSocket)
"""

import os
import json
import asyncio
import requests
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Set, Tuple, Optional
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
import websockets
from collections import defaultdict

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ==================== CLOB V2 CLIENT ====================
try:
    from py_clob_client_v2 import (
        ClobClient, OrderArgs, MarketOrderArgs, OrderType, Side, ApiCreds, PartialCreateOrderOptions,
    )
    CLOB_AVAILABLE = True
    logging.info("✅ py_clob_client_v2 loaded successfully")
except ImportError:
    CLOB_AVAILABLE = False
    logging.warning("py_clob_client_v2 not installed — running in simulation mode.")

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logging.warning("psycopg2 not installed — seen_trades will fall back to local file.")

# ==================== CONFIG ====================
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

WALLETS = { ... }  # ← Your original WALLETS dict remains unchanged

YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
POLY_API_KEY     = os.getenv("POLY_API_KEY", "")
POLY_SECRET      = os.getenv("POLY_SECRET", "")
POLY_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")
DATABASE_URL     = os.getenv("DATABASE_URL", "")

INITIAL_BANKROLL      = 10.0
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "20"))
POLL_INTERVAL         = 15
COMPOUNDING_RATE      = float(os.getenv("COMPOUNDING_RATE", "0.70"))
MAX_DRAWDOWN          = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT           = int(os.getenv("PORT", "8080"))
PAUSE_HOURS           = 48
MAX_RETRIES           = 3
RETRY_DELAY           = 5
MAX_SLIPPAGE          = float(os.getenv("MAX_SLIPPAGE", "0.20"))
LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))
LIMIT_EXPIRY_SECONDS  = int(os.getenv("LIMIT_EXPIRY_SECONDS", "300"))
SEEN_TRADES_FILE      = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")
MAX_FILL_CHECK_ERRORS = int(os.getenv("MAX_FILL_CHECK_ERRORS", "5"))
PUSD_CONTRACT_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
SELL_SETTLE_WAIT      = int(os.getenv("SELL_SETTLE_WAIT", "8"))

current_bankroll      = INITIAL_BANKROLL
peak_bankroll         = INITIAL_BANKROLL
compounding_bankroll  = INITIAL_BANKROLL
bot_paused_until: Optional[datetime] = None
_trade_lock = threading.Lock()

# ==================== WEBSOCKET MARKET DATA MANAGER ====================
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
                    logging.info("✅ WebSocket connected to Polymarket CLOB")

                    if self.subscribed_tokens:
                        await self._subscribe(list(self.subscribed_tokens))

                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            self._handle_message(data)
                        except Exception as e:
                            logging.debug(f"WS parse error: {e}")

            except Exception as e:
                logging.warning(f"WebSocket error: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)

    async def _subscribe(self, token_ids: list):
        if not self.ws or not token_ids:
            return
        try:
            msg = {"assets_ids": token_ids, "type": "market"}
            await self.ws.send(json.dumps(msg))
            self.subscribed_tokens.update(token_ids)
            logging.info(f"WS subscribed to {len(token_ids)} tokens")
        except Exception as e:
            logging.warning(f"Subscribe failed: {e}")

    def _handle_message(self, data: dict):
        asset_id = data.get("asset_id")
        if not asset_id:
            return

        if data.get("event_type") in ("price_change", "last_trade_price", "book"):
            price = data.get("price") or data.get("last_trade_price")
            if price:
                try:
                    self.token_to_price[asset_id] = round(float(price), 6)
                except:
                    pass

    def get_current_price(self, token_id: str) -> float:
        return self.token_to_price.get(token_id, 0.0)

    async def update_subscriptions(self, active_tokens: Set[str]):
        new_tokens = active_tokens - self.subscribed_tokens
        if new_tokens and self.ws:
            await self._subscribe(list(new_tokens))


market_data = MarketDataManager()

# ==================== DASHBOARD (unchanged) ====================
# ... [Your original HTML_TEMPLATE and build_dashboard function remain exactly the same] ...

# (Copy-paste your original HTML_TEMPLATE, build_dashboard, HealthHandler, _bot_ref, run_health_server here)

# ==================== DATA CLASSES (unchanged) ====================
# ... [Position, PendingLimitBuy] ...

# ==================== SEEN TRADES, BALANCE MANAGER, EXECUTOR (unchanged) ====================
# ... [SeenTradesStore, RobustBalanceManager, PolymarketExecutor] copy as-is from your original code ...

# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run    = dry_run
        self.balance    = RobustBalanceManager()
        self.positions: Dict[str, Position]        = {}
        self.pending:   Dict[str, PendingLimitBuy] = {}
        self.executor   = PolymarketExecutor(dry_run)
        self.seen       = SeenTradesStore(SEEN_TRADES_FILE, DATABASE_URL)
        self.market_data = market_data

        self._first_scan_done: Set[str] = set()
        self.closed_positions: list     = []

        logging.info(f"Multi-Wallet CopyTrader V2 (WebSocket + 15s fallback) | mode={'DRY' if dry_run else 'LIVE'}")

    def _reserved_capital(self) -> float:
        in_positions = sum(p.size_usd for p in self.positions.values())
        in_pending   = sum(p.size_usd for p in self.pending.values())
        return in_positions + in_pending

    def _available_balance(self) -> float:
        bal = self.balance.cached_balance or 0.0
        return max(0.0, bal - self._reserved_capital())

    def _can_afford(self, amount_usd: float) -> bool:
        available = self._available_balance()
        can = available >= amount_usd * 1.02
        if not can:
            logging.warning(f"Affordability check failed: need ${amount_usd:.2f} | available=${available:.2f}")
        return can

    # Keep all your original helper methods: get_orderbook_prices, _get_best_bid, get_risk_percent, check_drawdown, _get_positions

    def get_orderbook_prices(self, token_id: str) -> Tuple[float, float]:
        # Your original implementation
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8)
                if r.status_code == 200:
                    data = r.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    best_bid = float(bids[0]["price"]) if bids else 0.0
                    best_ask = float(asks[0]["price"]) if asks else 0.0
                    mid = (best_bid + best_ask) / 2 if best_bid and best_ask else best_bid or best_ask
                    return mid, best_ask
            except Exception:
                pass
        return 0.0, 0.0

    def _get_best_bid(self, token_id: str) -> float:
        # Your original implementation
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8)
                if r.status_code == 200:
                    bids = r.json().get("bids", [])
                    return float(bids[0]["price"]) if bids else 0.0
            except Exception:
                pass
        return 0.0

    def get_risk_percent(self, price: float, config: dict) -> float:
        if config.get("risk_type") == "fixed":
            return config.get("fixed_risk", 0.025)
        if price >= 0.70:
            return 0.03
        elif price >= 0.30:
            return 0.01
        else:
            return 0.006

    def check_drawdown(self) -> bool:
        # Your original implementation (unchanged)
        global peak_bankroll, bot_paused_until
        current = self.balance.get_balance()
        if current > peak_bankroll:
            peak_bankroll = current
        dd = (peak_bankroll - current) / peak_bankroll if peak_bankroll > 0 else 0
        if dd >= MAX_DRAWDOWN:
            if bot_paused_until is None or datetime.now() > bot_paused_until:
                bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
                logging.warning(f"DRAWDOWN PROTECTION TRIGGERED ({dd*100:.1f}%) — paused {PAUSE_HOURS}h")
            return True
        return False

    def _get_positions(self, wallet_addr: str):
        # Your original implementation (unchanged)
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50", timeout=12)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    time.sleep(int(resp.headers.get("Retry-After", 30)))
            except Exception as e:
                logging.warning(f"Position fetch error: {e}")
                time.sleep(RETRY_DELAY)
        return None

    # ==================== MAIN SCAN LOGIC ====================
    async def scan_and_copy(self):
        global current_bankroll, compounding_bankroll, bot_paused_until

        if bot_paused_until and datetime.now() < bot_paused_until:
            remaining = (bot_paused_until - datetime.now()).seconds // 60
            logging.info(f"Bot paused — {remaining} minutes remaining")
            return

        if self.check_drawdown():
            return

        current_bankroll = self.balance.get_balance()
        if current_bankroll is None:
            logging.error("Real pUSD balance unavailable — skipping scan cycle")
            return

        logging.info(f"Scanning | WS: {'Connected' if self.market_data.ws else 'Disconnected'} | "
                    f"balance=${current_bankroll:.2f} | open={len(self.positions)} | pending={len(self.pending)}")

        source_token_ids_by_wallet: Dict[str, set] = {}

        for wallet_addr, config in WALLETS.items():
            raw = self._get_positions(wallet_addr)
            if raw is None:
                logging.warning(f"Skipping {config['name']} — could not fetch positions")
                continue

            source_token_ids: set = set()
            source_shares_map: Dict[str, float] = {}

            for pos in raw:
                tid = pos.get("asset", "")
                shares = float(pos.get("size", pos.get("shares", 0)))
                if tid and shares > 0:
                    source_token_ids.add(tid)
                    source_shares_map[tid] = shares

            # First scan logic (unchanged)
            if wallet_addr not in self._first_scan_done:
                self._first_scan_done.add(wallet_addr)
                if config.get("copy_mode") == "new_only":
                    all_keys = {f"{wallet_addr}_{tid}" for tid in source_token_ids}
                    self.seen.snapshot_existing(all_keys)
                    source_token_ids_by_wallet[wallet_addr] = source_token_ids
                    continue

            for pos in raw:
                # === Your original buy decision logic (unchanged) ===
                token_id = pos.get("asset", "")
                market_id = pos.get("conditionId", "")
                question = pos.get("title", "Unknown")
                outcome = pos.get("outcome", "YES")
                size_usd = float(pos.get("currentValue", 0))
                source_shares_at_copy = float(pos.get("size", pos.get("shares", 0)))

                min_value = 0.0 if config.get("copy_sub_dollar") else 1.0
                if not token_id or size_usd < min_value or size_usd <= 0:
                    continue

                pos_key = f"{wallet_addr}_{token_id}"
                if (self.seen.is_seen(pos_key) or 
                    pos_key in self.positions or 
                    pos_key in self.pending):
                    continue

                if len(self.positions) + len(self.pending) >= MAX_POSITIONS:
                    break

                # === WebSocket Price with API fallback ===
                cur_price = self.market_data.get_current_price(token_id)
                if cur_price <= 0:
                    cur_price = float(pos.get("curPrice", 0))

                if cur_price <= 0:
                    continue

                wallet_premium = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
                limit_price = round(cur_price, 4)

                if config.get("copy_sub_dollar") and size_usd < 1.0:
                    my_size = round(size_usd, 2)
                else:
                    risk_pct = self.get_risk_percent(limit_price, config)
                    my_size = round(min(compounding_bankroll * risk_pct, self._available_balance() * 0.95), 2)

                if my_size <= 0 or not self._can_afford(my_size):
                    continue

                ok, order_id, actual_price = self.executor.place_limit_buy(token_id, my_size, limit_price)
                if ok:
                    self.seen.mark_seen(pos_key)
                    self.pending[pos_key] = PendingLimitBuy(
                        pos_key=pos_key, token_id=token_id, market_id=market_id, question=question,
                        outcome=outcome, source_wallet=wallet_addr, source_name=config["name"],
                        limit_price=actual_price, size_usd=my_size, order_id=order_id,
                        source_shares=source_shares_at_copy
                    )

            source_token_ids_by_wallet[wallet_addr] = source_token_ids

            # Update open positions prices
            for p in self.positions.values():
                if p.source_wallet == wallet_addr:
                    ws_price = self.market_data.get_current_price(p.token_id)
                    if ws_price > 0:
                        p.current_price = ws_price

        self._process_pending_orders(source_token_ids_by_wallet)

        # Dynamic WebSocket subscription
        all_active_tokens = {p.token_id for p in self.positions.values()} | \
                            {p.token_id for p in self.pending.values()}
        await self.market_data.update_subscriptions(all_active_tokens)

    # Keep all your original methods below unchanged:
    # _process_pending_orders, _execute_sell, run(), etc.

    # (Paste the rest of your original CopyTrader class here - _process_pending_orders through run())

# ==================== ENTRY POINT ====================
async def main():
    global _bot_ref

    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    bot = CopyTrader(dry_run=DRY_RUN)
    _bot_ref = bot

    ws_task = asyncio.create_task(market_data.connect())

    try:
        starting_balance = bot.balance.fetch_with_retry(retries=5, delay=10)
        bot.balance.peak_balance = starting_balance
        global peak_bankroll, compounding_bankroll
        peak_bankroll = starting_balance
        compounding_bankroll = starting_balance

        await bot.run()
    finally:
        market_data.running = False
        ws_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
