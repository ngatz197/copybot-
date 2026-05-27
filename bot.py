#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - PRODUCTION READY (py_clob_client_v2)
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

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ==================== CLOB CLIENT V2 ====================
try:
    from py_clob_client_v2 import ClobClient, OrderArgs, MarketOrderArgs, OrderType
    from py_clob_client_v2 import ApiCreds, PartialCreateOrderOptions, Side
    CLOB_AVAILABLE = True
    logging.info("✅ py_clob_client_v2 loaded successfully")
except ImportError:
    CLOB_AVAILABLE = False
    logging.warning("py_clob_client_v2 not installed — running in simulation mode.")

# ==================== CONFIG ====================
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

WALLETS = {
    "0x0c0e270cf879583d6a0142fc817e05b768d0434e": {"name": "TheSpirit", "risk_type": "price_based"},
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {"name": "WalletA179", "risk_type": "fixed", "fixed_risk": 0.025},
}

YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
POLY_API_KEY     = os.getenv("POLY_API_KEY", "")
POLY_SECRET      = os.getenv("POLY_SECRET", "")
POLY_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")

MAX_POSITIONS     = int(os.getenv("MAX_POSITIONS", "8"))
POLL_INTERVAL     = int(os.getenv("POLL_SECONDS", "40"))
MAX_DRAWDOWN      = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT       = int(os.getenv("PORT", "8080"))
PAUSE_HOURS       = 48
MAX_RETRIES       = 3
RETRY_DELAY       = 5

LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))
LIMIT_EXPIRY_SECONDS  = int(os.getenv("LIMIT_EXPIRY_SECONDS", "300"))
SEEN_TRADES_FILE      = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")

current_bankroll  = 10.0
peak_bankroll     = 10.0
bot_paused_until: Optional[datetime] = None


# ==================== DASHBOARD ====================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>CopyTrader Dashboard</title>
    <meta http-equiv="refresh" content="15">
    <style>
        body { font-family: 'Segoe UI', Arial, sans-serif; background: #0a0a0a; color: #00cc00; margin: 0; padding: 20px; }
        h1 { color: #00ff00; text-align: center; }
        .container { max-width: 1100px; margin: auto; }
        .card { background: #111111; border-radius: 10px; padding: 20px; margin-bottom: 20px; box-shadow: 0 0 10px rgba(0,255,0,0.1); }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #222; }
        th { background: #1a1a1a; }
        .green { color: #00ff88; }
        .red { color: #ff4444; }
        .status { font-size: 1.2em; font-weight: bold; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Polymarket CopyTrader</h1>
        <div class="card">
            <h2>Status: <span class="status" style="color:{status_color};">{status}</span></h2>
            <p><strong>Mode:</strong> {mode} | <strong>Last Updated:</strong> {last_updated}</p>
            <p><strong>Bankroll:</strong> ${bankroll:.2f} | <strong>Peak:</strong> ${peak:.2f}</p>
            <p><strong>Drawdown:</strong> <span class="{dd_class}">{drawdown:.1f}%</span></p>
            <p><strong>Open:</strong> {open_pos}/{max_pos} | <strong>Pending:</strong> {pending_pos} | <strong>Seen:</strong> {seen_count}</p>
        </div>
        <div class="card">
            <h2>Open Positions</h2>
            {positions_table}
        </div>
        <div class="card">
            <h2>Pending Orders</h2>
            {pending_table}
        </div>
    </div>
</body>
</html>
"""

def build_dashboard(bot) -> dict:
    bankroll = bot.balance.cached_balance or 0.0
    drawdown = ((peak_bankroll - bankroll) / peak_bankroll * 100) if peak_bankroll > 0 else 0
    status = "PAUSED" if bot_paused_until and datetime.now() < bot_paused_until else "RUNNING"
    status_color = "#ff4444" if status == "PAUSED" else "#00ff88"
    dd_class = "red" if drawdown > 5 else "green"

    pos_rows = "".join(f"<tr><td>{p.source_name}</td><td>{p.question[:50]}</td><td>{p.outcome}</td><td>${p.size_usd:.2f}</td><td>{p.entry_price:.3f}</td><td>{p.status}</td></tr>" for p in bot.positions.values())
    pos_table = f"<table><tr><th>Source</th><th>Market</th><th>Outcome</th><th>Size</th><th>Entry</th><th>Status</th></tr>{pos_rows}</table>" if pos_rows else "<p>No open positions</p>"

    pend_rows = "".join(f"<tr><td>{p.source_name}</td><td>{p.question[:50]}</td><td>${p.size_usd:.2f}</td><td>{p.limit_price:.3f}</td><td>{(datetime.now()-p.placed_at).seconds}s</td></tr>" for p in bot.pending.values())
    pend_table = f"<table><tr><th>Source</th><th>Market</th><th>Size</th><th>Limit</th><th>Age</th></tr>{pend_rows}</table>" if pend_rows else "<p>No pending orders</p>"

    return {
        "status": status, "status_color": status_color, "mode": "LIVE" if not bot.dry_run else "DRY RUN",
        "bankroll": bankroll, "peak": peak_bankroll, "drawdown": drawdown, "dd_class": dd_class,
        "open_pos": len(bot.positions), "max_pos": MAX_POSITIONS, "pending_pos": len(bot.pending),
        "seen_count": len(getattr(bot.seen, '_seen', [])), "positions_table": pos_table, "pending_table": pend_table,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" and _bot_ref:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            try:
                data = build_dashboard(_bot_ref)
                self.wfile.write(HTML_TEMPLATE.format(**data).encode())
            except:
                self.wfile.write(b"<h1>Dashboard loading...</h1>")
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK - CopyTrader running")
    def log_message(self, format, *args): pass

_bot_ref = None

def run_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    logging.info(f"🌐 Dashboard live at port {HEALTH_PORT}")
    server.serve_forever()


# ==================== DATA CLASSES ====================
@dataclass
class Position:
    market_id: str; question: str; outcome: str; token_id: str; entry_price: float
    size_usd: float; shares: float; source_wallet: str; source_name: str
    status: str = "open"; exit_price: float = 0.0; pnl: float = 0.0; order_id: str = ""

@dataclass
class PendingLimitBuy:
    pos_key: str; token_id: str; market_id: str; question: str; outcome: str
    source_wallet: str; source_name: str; limit_price: float; size_usd: float
    order_id: str; placed_at: datetime = field(default_factory=datetime.now)


# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    USDC_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
    POLYGON_RPCS = ["https://polygon-bor-rpc.publicnode.com", "https://polygon.llamarpc.com", "https://polygon.drpc.org"]

    def __init__(self):
        self.cached_balance: Optional[float] = None
        self.last_update = 0
        self.peak_balance = 0.0

    def _fetch_balance(self) -> float:
        if not YOUR_WALLET: return 0.0
        padded = YOUR_WALLET.lower().replace("0x", "").zfill(64)
        payload = {"jsonrpc": "2.0", "method": "eth_call", "params": [{"to": self.USDC_ADDRESS, "data": "0x70a08231" + padded}, "latest"], "id": 1}
        for rpc in self.POLYGON_RPCS:
            try:
                resp = requests.post(rpc, json=payload, timeout=8)
                if resp.status_code == 200:
                    result = resp.json().get("result", "0x0")
                    if result not in ("0x", "0x0"):
                        balance = int(result, 16) / 1_000_000
                        if balance > 0: return balance
            except: continue
        return 0.0

    def get_balance(self, force=False) -> Optional[float]:
        if force or self.cached_balance is None or (time.time() - self.last_update > 30):
            real = self._fetch_balance()
            if real > 0:
                self.cached_balance = real
                self.last_update = time.time()
                if real > self.peak_balance: self.peak_balance = real
        return self.cached_balance

    def fetch_with_retry(self, retries: int = 5, delay: int = 10) -> float:
        for _ in range(retries):
            val = self._fetch_balance()
            if val > 0:
                self.cached_balance = val
                self.peak_balance = val
                self.last_update = time.time()
                logging.info(f"Real balance confirmed: ${val:.2f}")
                return val
            time.sleep(delay)
        raise RuntimeError("Could not fetch real balance after retries.")

    def check_drawdown(self) -> Tuple[bool, float]:
        current = self.get_balance()
        if current is None or self.peak_balance == 0: return False, 0.0
        dd = (self.peak_balance - current) / self.peak_balance
        return dd >= MAX_DRAWDOWN, dd


# ==================== EXECUTOR V2 ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client = None
        if not dry_run and CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(api_key=POLY_API_KEY, api_secret=POLY_SECRET, api_passphrase=POLY_PASSPHRASE) if POLY_API_KEY else None
                self.client = ClobClient(host="https://clob.polymarket.com", chain_id=137, key=YOUR_PRIVATE_KEY, creds=creds, funder=YOUR_WALLET)
                logging.info("✅ ClobClient v2 initialized successfully")
            except Exception as e:
                logging.error(f"ClobClient v2 init failed: {e}")

    def place_limit_buy(self, token_id: str, amount_usd: float, target_price: float, source_price: float) -> Tuple[bool, str, float]:
        price_cap = round(min(source_price * (1 + LIMIT_BUY_MAX_PREMIUM), 0.90), 4)
        limit_price = round(min(target_price, price_cap), 4)
        size = round(amount_usd / limit_price, 4)

        if self.dry_run or not self.client:
            logging.info(f"[DRY RUN] LIMIT BUY {size:.4f} @ {limit_price:.4f}")
            return True, "dry-run-limit-buy", limit_price

        for attempt in range(MAX_RETRIES):
            try:
                order_args = OrderArgs(token_id=token_id, price=limit_price, side=Side.BUY, size=size)
                result = self.client.create_and_post_order(order_args=order_args, options=PartialCreateOrderOptions(tick_size="0.01"), order_type=OrderType.GTC)
                order_id = result.get("orderID") or result.get("id") or "unknown"
                logging.info(f"LIMIT BUY placed: {order_id}")
                return True, order_id, limit_price
            except Exception as e:
                logging.warning(f"Limit buy attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return False, "", limit_price

    def cancel_order(self, order_id: str) -> bool:
        if self.dry_run or not self.client: return True
        try:
            self.client.cancel_order(order_id)
            return True
        except: return False

    def is_order_filled(self, order_id: str) -> bool:
        if self.dry_run or not self.client: return True
        try:
            order = self.client.get_order(order_id)
            return order.get("status", "").lower() in ("matched", "filled", "success")
        except: return False

    def place_sell(self, token_id: str, shares: float) -> Tuple[bool, str]:
        if self.dry_run or not self.client:
            return True, "dry-run-sell"
        for attempt in range(MAX_RETRIES):
            try:
                order_args = MarketOrderArgs(token_id=token_id, amount=shares, side=Side.SELL)
                result = self.client.create_and_post_market_order(order_args=order_args, options=PartialCreateOrderOptions(tick_size="0.01"))
                return True, result.get("orderID") or "unknown"
            except Exception as e:
                logging.warning(f"Sell attempt failed: {e}")
                time.sleep(RETRY_DELAY)
        return False, ""


# ==================== SEEN TRADES STORE ====================
class SeenTradesStore:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self._seen: Set[str] = self._load()
        logging.info(f"SeenTradesStore loaded {len(self._seen)} entries")

    def _load(self) -> Set[str]:
        try:
            with open(self.filepath) as f:
                return set(json.load(f))
        except: return set()

    def _save(self):
        try:
            with open(self.filepath, "w") as f:
                json.dump(sorted(self._seen), f)
        except: pass

    def is_seen(self, pos_key: str) -> bool:
        return pos_key in self._seen

    def mark_seen(self, pos_key: str):
        if pos_key not in self._seen:
            self._seen.add(pos_key)
            self._save()

    def snapshot_existing(self, pos_keys):
        added = sum(1 for k in pos_keys if k not in self._seen)
        if added:
            self._seen.update(pos_keys)
            self._save()
            logging.info(f"Snapshotted {added} existing trades")

    @property
    def is_empty(self) -> bool:
        return len(self._seen) == 0


# ==================== COPY TRADER (Full Methods) ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.balance = RobustBalanceManager()
        self.positions: Dict[str, Position] = {}
        self.pending: Dict[str, PendingLimitBuy] = {}
        self.executor = PolymarketExecutor(dry_run)
        self.seen = SeenTradesStore(SEEN_TRADES_FILE)
        self._first_scan: Set[str] = set()

        logging.info("CopyTrader initialized")

    def get_orderbook_prices(self, token_id: str) -> Tuple[float, float]:
        for _ in range(MAX_RETRIES):
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
            except: pass
        return 0.0, 0.0

    def _source_entry_price(self, pos: dict, best_ask: float, mid_price: float) -> float:
        for key in ("avgPrice", "averagePrice", "avg_price", "average_price"):
            if val := pos.get(key): return float(val)
        for key in ("curPrice", "currentPrice", "cur_price", "price"):
            if val := pos.get(key): return float(val)
        return best_ask if best_ask > 0 else mid_price

    def get_risk_percent(self, price: float, config: dict) -> float:
        if config.get("risk_type") == "fixed":
            return config.get("fixed_risk", 0.025)
        if price >= 0.70: return 0.03
        elif price >= 0.30: return 0.01
        return 0.006

    def check_drawdown(self) -> bool:
        global peak_bankroll, bot_paused_until
        current = self.balance.get_balance()
        if current > peak_bankroll: peak_bankroll = current
        dd = (peak_bankroll - current) / peak_bankroll if peak_bankroll > 0 else 0
        if dd >= MAX_DRAWDOWN:
            if not bot_paused_until or datetime.now() > bot_paused_until:
                bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
                logging.warning(f"DRAWDOWN PROTECTION TRIGGERED ({dd*100:.1f}%)")
            return True
        return False

    def _get_positions(self, wallet_addr: str):
        for _ in range(MAX_RETRIES):
            try:
                resp = requests.get(f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50", timeout=12)
                if resp.status_code == 200: return resp.json()
            except: pass
        return None

    def _process_pending_orders(self, source_token_ids_by_wallet: Dict[str, set]):
        for pos_key, pending in list(self.pending.items()):
            if pending.token_id not in source_token_ids_by_wallet.get(pending.source_wallet, set()):
                self.executor.cancel_order(pending.order_id)
                del self.pending[pos_key]
                continue

            if self.executor.is_order_filled(pending.order_id):
                shares = pending.size_usd / pending.limit_price if pending.limit_price > 0 else 0
                self.positions[pos_key] = Position(market_id=pending.market_id, question=pending.question, outcome=pending.outcome,
                    token_id=pending.token_id, entry_price=pending.limit_price, size_usd=pending.size_usd, shares=shares,
                    source_wallet=pending.source_wallet, source_name=pending.source_name, order_id=pending.order_id)
                del self.pending[pos_key]
                logging.info(f"LIMIT BUY FILLED: {pending.question[:40]}")
                continue

            if (datetime.now() - pending.placed_at).total_seconds() >= LIMIT_EXPIRY_SECONDS:
                self.executor.cancel_order(pending.order_id)
                del self.pending[pos_key]
                # Retry logic can be added later

    async def scan_and_copy(self):
        global current_bankroll, bot_paused_until, peak_bankroll
        if bot_paused_until and datetime.now() < bot_paused_until: return
        if self.check_drawdown(): return

        current_bankroll = self.balance.get_balance()
        if current_bankroll is None: return

        source_token_ids_by_wallet = {}

        for wallet_addr, config in WALLETS.items():
            raw = self._get_positions(wallet_addr)
            if not raw: continue

            source_token_ids = {pos.get("asset") for pos in raw if float(pos.get("value", 0)) >= 1.0}

            # First scan logic
            if wallet_addr not in self._first_scan:
                self._first_scan.add(wallet_addr)
                if self.seen.is_empty:
                    self.seen.snapshot_existing({f"{wallet_addr}_{tid}" for tid in source_token_ids})

            # BUY LOGIC
            for pos in raw:
                token_id = pos.get("asset")
                if not token_id or float(pos.get("value", 0)) < 1.0: continue
                pos_key = f"{wallet_addr}_{token_id}"
                if self.seen.is_seen(pos_key) or pos_key in self.positions or pos_key in self.pending: continue
                if len(self.positions) + len(self.pending) >= MAX_POSITIONS: break

                mid_price, best_ask = self.get_orderbook_prices(token_id)
                if mid_price <= 0: continue

                target_price = self._source_entry_price(pos, best_ask, mid_price)
                price_cap = round(min(target_price * (1 + LIMIT_BUY_MAX_PREMIUM), 0.90), 4)
                if mid_price > price_cap:
                    self.seen.mark_seen(pos_key)
                    continue

                risk_pct = self.get_risk_percent(target_price, config)
                my_size = round(current_bankroll * risk_pct, 2)
                if my_size < 1.0: continue

                ok, order_id, limit_price = self.executor.place_limit_buy(token_id, my_size, target_price, target_price)
                if ok:
                    self.seen.mark_seen(pos_key)
                    self.pending[pos_key] = PendingLimitBuy(pos_key=pos_key, token_id=token_id, market_id=pos.get("market",""),
                        question=pos.get("title",""), outcome=pos.get("outcome",""), source_wallet=wallet_addr,
                        source_name=config["name"], limit_price=limit_price, size_usd=my_size, order_id=order_id)

            source_token_ids_by_wallet[wallet_addr] = source_token_ids

            # SELL LOGIC
            for pos_key, position in list(self.positions.items()):
                if position.source_wallet == wallet_addr and position.token_id not in source_token_ids:
                    ok, _ = self.executor.place_sell(position.token_id, position.shares)
                    if ok:
                        del self.positions[pos_key]

        self._process_pending_orders(source_token_ids_by_wallet)

    async def run(self):
        logging.info("Bot loop started")
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Loop error: {e}")
            await asyncio.sleep(POLL_INTERVAL)


# ==================== ENTRY POINT ====================
async def main():
    global _bot_ref, peak_bankroll

    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    bot = CopyTrader(dry_run=DRY_RUN)
    _bot_ref = bot

    logging.info(f"seen_trades.json contains {len(bot.seen._seen)} entries")

    try:
        starting_balance = bot.balance.fetch_with_retry()
        peak_bankroll = starting_balance
    except Exception as e:
        logging.error(f"Balance fetch failed: {e}")

    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
