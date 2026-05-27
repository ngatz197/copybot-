#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - PRODUCTION READY
- Limit Buy Orders priced at source wallet's avg entry price, capped at 0.80
- Falls back to best_ask (then mid) if source entry price unavailable
- Market Sell Orders (instant exit)
- Real Mid-Price Fetching
- 20% Drawdown Protection
- Improved Balance Fetching + Robust Error Handling & Retries
- Pending limit order tracking + auto-cancel on expiry
- Health endpoint for Render (keeps bot awake)
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

# ==================== CLOB CLIENT ====================
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import LimitOrderArgs, MarketOrderArgs, OrderType
    from py_clob_client.constants import POLYGON
    CLOB_AVAILABLE = True
    logging.info("✅ py_clob_client loaded successfully")
except ImportError:
    CLOB_AVAILABLE = False
    logging.warning("py_clob_client not installed — running in simulation mode.")

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

INITIAL_BANKROLL  = 10.0
MAX_POSITIONS     = int(os.getenv("MAX_POSITIONS", "8"))
POLL_INTERVAL     = int(os.getenv("POLL_SECONDS", "40"))
COMPOUNDING_RATE  = float(os.getenv("COMPOUNDING_RATE", "0.70"))
MAX_DRAWDOWN      = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT       = int(os.getenv("PORT", "8080"))
PAUSE_HOURS       = 48
MAX_RETRIES       = 3
RETRY_DELAY       = 5

# ---- Limit order settings ----
# Max % above the source wallet's entry price we are willing to pay (0.20 = 20%)
LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))

# How many seconds before an unfilled limit buy is cancelled and retried
LIMIT_EXPIRY_SECONDS  = int(os.getenv("LIMIT_EXPIRY_SECONDS", "300"))  # 5 minutes

# Persistent file tracking every trade we have ever seen/copied (survives restarts)
SEEN_TRADES_FILE      = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")

current_bankroll  = INITIAL_BANKROLL
peak_bankroll     = INITIAL_BANKROLL
bot_paused_until: Optional[datetime] = None


# ==================== DASHBOARD ====================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>CopyTrader Live Dashboard</title>
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
        <h1>🤖 Polymarket CopyTrader Dashboard</h1>
        <div class="card">
            <h2>Status: <span class="status" style="color:{status_color};">{status}</span></h2>
            <p><strong>Mode:</strong> {mode} | <strong>Last Updated:</strong> {last_updated}</p>
            <p><strong>Bankroll:</strong> ${bankroll:.2f} | <strong>Peak:</strong> ${peak:.2f}</p>
            <p><strong>Drawdown:</strong> <span class="{dd_class}">{drawdown:.1f}%</span></p>
            <p><strong>Open Positions:</strong> {open_pos} / {max_pos} | <strong>Pending:</strong> {pending_pos}</p>
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

_dashboard_cache: dict = {}

def build_dashboard(bot) -> dict:
    bankroll = bot.balance.cached_balance or 0.0
    drawdown = ((peak_bankroll - bankroll) / peak_bankroll * 100) if peak_bankroll > 0 else 0
    status = "PAUSED" if bot_paused_until and datetime.now() < bot_paused_until else "RUNNING"
    status_color = "#ff4444" if status == "PAUSED" else "#00ff88"
    dd_class = "red" if drawdown > 5 else "green"

    pos_rows = ""
    for p in bot.positions.values():
        pos_rows += f"<tr><td>{p.source_name}</td><td>{p.question[:50]}</td><td>{p.outcome}</td><td>${p.size_usd:.2f}</td><td>{p.entry_price:.3f}</td><td>{p.status}</td></tr>"
    pos_table = f"<table><tr><th>Source</th><th>Market</th><th>Outcome</th><th>Size</th><th>Entry</th><th>Status</th></tr>{pos_rows}</table>" if pos_rows else "<p>No open positions</p>"

    pend_rows = ""
    for p in bot.pending.values():
        age = (datetime.now() - p.placed_at).seconds
        pend_rows += f"<tr><td>{p.source_name}</td><td>{p.question[:50]}</td><td>${p.size_usd:.2f}</td><td>{p.limit_price:.3f}</td><td>{age}s</td></tr>"
    pend_table = f"<table><tr><th>Source</th><th>Market</th><th>Size</th><th>Limit Price</th><th>Age</th></tr>{pend_rows}</table>" if pend_rows else "<p>No pending orders</p>"

    return {
        "status": status,
        "status_color": status_color,
        "mode": "LIVE" if not bot.dry_run else "DRY RUN",
        "bankroll": bankroll,
        "peak": peak_bankroll,
        "drawdown": drawdown,
        "dd_class": dd_class,
        "open_pos": len(bot.positions),
        "max_pos": MAX_POSITIONS,
        "pending_pos": len(bot.pending),
        "positions_table": pos_table,
        "pending_table": pend_table,
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
                html = HTML_TEMPLATE.format(**data)
                self.wfile.write(html.encode())
            except Exception:
                self.wfile.write(b"<h1>Dashboard loading...</h1>")
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK - CopyTrader running")

    def log_message(self, format, *args):
        pass


_bot_ref = None

def run_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    logging.info(f"🌐 Dashboard live at http://0.0.0.0:{HEALTH_PORT}")
    server.serve_forever()


# ==================== DATA CLASSES ====================
@dataclass
class Position:
    market_id:     str
    question:      str
    outcome:       str
    token_id:      str
    entry_price:   float
    size_usd:      float
    shares:        float
    source_wallet: str
    source_name:   str
    status:        str   = "open"
    exit_price:    float = 0.0
    pnl:           float = 0.0
    order_id:      str   = ""


@dataclass
class PendingLimitBuy:
    """Tracks an open limit buy order that has not yet been confirmed filled."""
    pos_key:       str
    token_id:      str
    market_id:     str
    question:      str
    outcome:       str
    source_wallet: str
    source_name:   str
    limit_price:   float
    size_usd:      float
    order_id:      str
    placed_at:     datetime = field(default_factory=datetime.now)


# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    USDC_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"  # PUSD
    POLYGON_RPCS = [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com",
        "https://polygon.drpc.org",
    ]

    def __init__(self):
        self.cached_balance: Optional[float] = None  # None = not yet fetched
        self.last_update    = 0
        self.peak_balance   = 0.0

    def _fetch_balance(self) -> float:
        """
        Fetches USDC balance directly from the Polygon blockchain via RPC.
        Calls balanceOf(address) on the USDC contract.
        Returns balance as float > 0, or 0.0 on failure.
        """
        if not YOUR_WALLET:
            logging.error("DEPOSIT_WALLET_ADDRESS not set — cannot fetch balance")
            return 0.0

        padded  = YOUR_WALLET.lower().replace("0x", "").zfill(64)
        payload = {
            "jsonrpc": "2.0",
            "method":  "eth_call",
            "params":  [
                {"to": self.USDC_ADDRESS, "data": "0x70a08231" + padded},
                "latest"
            ],
            "id": 1,
        }

        for rpc in self.POLYGON_RPCS:
            try:
                resp = requests.post(rpc, json=payload, timeout=8)
                logging.info(f"RPC {rpc} status={resp.status_code}")
                if resp.status_code == 200:
                    data   = resp.json()
                    logging.info(f"RPC response: {data}")
                    result = data.get("result", "0x0")
                    if result and result not in ("0x", "0x0"):
                        balance = int(result, 16) / 1_000_000  # USDC has 6 decimals
                        logging.info(f"Balance fetched via RPC ({rpc}): ${balance:.2f}")
                        if balance > 0:
                            return balance
                        else:
                            logging.warning(f"Balance is 0 for wallet {YOUR_WALLET[:10]}...")
            except Exception as e:
                logging.warning(f"RPC balance fetch failed ({rpc}): {e}")
                continue
        logging.error(f"All RPC attempts failed for wallet {YOUR_WALLET[:10] if YOUR_WALLET else 'NOT SET'}...")
        return 0.0

    def get_balance(self, force=False) -> Optional[float]:
        """
        Returns real balance from Polymarket, or cached value if fetched recently.
        Returns None if balance has never been successfully fetched.
        NEVER returns a simulated or hardcoded value.
        """
        if force or self.cached_balance is None or (time.time() - self.last_update > 30):
            real = self._fetch_balance()
            if real > 0:
                self.cached_balance = real
                self.last_update    = time.time()
                if real > self.peak_balance:
                    self.peak_balance = real
                    logging.info(f"New peak balance: ${self.peak_balance:.2f}")
            else:
                if self.cached_balance is None:
                    logging.error(
                        "Could not fetch real balance from Polymarket — "
                        "bot will not trade until balance is confirmed"
                    )
        return self.cached_balance  # may be None if never fetched successfully

    def fetch_with_retry(self, retries: int = 5, delay: int = 10) -> float:
        """
        Called at startup — blocks until a real balance is retrieved or retries exhausted.
        Raises RuntimeError if balance cannot be confirmed after all retries.
        """
        for attempt in range(1, retries + 1):
            val = self._fetch_balance()
            if val > 0:
                self.cached_balance = val
                self.peak_balance   = val
                self.last_update    = time.time()
                logging.info(f"Real balance confirmed: ${val:.2f}")
                return val
            logging.warning(f"Balance fetch attempt {attempt}/{retries} returned 0 — retrying in {delay}s")
            time.sleep(delay)
        raise RuntimeError(
            f"Could not fetch real balance from Polymarket after {retries} attempts. "
            "Check DEPOSIT_WALLET_ADDRESS and API connectivity."
        )

    def check_drawdown(self) -> Tuple[bool, float]:
        current = self.get_balance()
        if current is None or self.peak_balance == 0:
            return False, 0.0
        dd = (self.peak_balance - current) / self.peak_balance
        return dd >= MAX_DRAWDOWN, dd


# ==================== EXECUTOR ====================
class PolymarketExecutor:
    """
    Buys  → GTC limit orders priced at source wallet's avg entry, capped at LIMIT_BUY_PRICE_CAP (0.80)
    Sells → market orders for instant exit
    """

    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client  = None
        if not dry_run and CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                self.client = ClobClient(
                    host           = "https://clob.polymarket.com",
                    key            = YOUR_PRIVATE_KEY,
                    chain_id       = POLYGON,
                    api_key        = POLY_API_KEY,
                    api_secret     = POLY_SECRET,
                    api_passphrase = POLY_PASSPHRASE,
                )
                logging.info("ClobClient initialised — LIVE mode")
            except Exception as e:
                logging.error(f"ClobClient init failed: {e}")
                self.client = None

    # ---------- LIMIT BUY ----------
    def place_limit_buy(
        self, token_id: str, amount_usd: float, target_price: float, source_price: float
    ) -> Tuple[bool, str, float]:
        """
        Places a GTC limit buy.
        target_price  = source wallet's avg entry (or best_ask / mid fallback).
        Hard cap      = source_price * (1 + LIMIT_BUY_MAX_PREMIUM), also capped at 0.99.
        Returns (success, order_id, limit_price_used)
        """
        price_cap   = round(min(source_price * (1 + LIMIT_BUY_MAX_PREMIUM), 0.99), 4)
        limit_price = round(min(target_price, price_cap), 4)
        shares      = round(amount_usd / limit_price, 4)

        if self.dry_run or self.client is None:
            logging.info(
                f"[DRY RUN] LIMIT BUY {shares:.4f} shares @ {limit_price:.4f} "
                f"(${amount_usd:.2f}) token {token_id[:12]}…"
            )
            return True, "dry-run-limit-buy", limit_price

        for attempt in range(MAX_RETRIES):
            try:
                args = LimitOrderArgs(
                    token_id  = token_id,
                    price     = limit_price,
                    size      = shares,
                    side      = "BUY",
                )
                result   = self.client.create_and_post_order(args)
                order_id = result.get("orderID", "unknown")
                logging.info(
                    f"LIMIT BUY placed: {order_id} | {shares:.4f} shares @ {limit_price:.4f}"
                )
                return True, order_id, limit_price
            except Exception as e:
                logging.warning(f"LIMIT BUY attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return False, "", limit_price

    # ---------- CANCEL ORDER ----------
    def cancel_order(self, order_id: str) -> bool:
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] CANCEL order {order_id}")
            return True
        try:
            self.client.cancel(order_id)
            logging.info(f"Cancelled order {order_id}")
            return True
        except Exception as e:
            logging.warning(f"Cancel failed for {order_id}: {e}")
            return False

    # ---------- CHECK ORDER FILLED ----------
    def is_order_filled(self, order_id: str) -> bool:
        """Returns True if the order is fully filled."""
        if self.dry_run or self.client is None:
            return True  # simulate instant fill in dry-run
        try:
            order = self.client.get_order(order_id)
            status = order.get("status", "").lower()
            return status in ("matched", "filled")
        except Exception as e:
            logging.warning(f"Could not check order status for {order_id}: {e}")
            return False

    # ---------- MARKET SELL ----------
    def place_sell(self, token_id: str, shares: float) -> Tuple[bool, str]:
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] MARKET SELL {shares:.4f} shares token {token_id[:12]}…")
            return True, "dry-run-sell"
        for attempt in range(MAX_RETRIES):
            try:
                args     = MarketOrderArgs(token_id=token_id, amount=shares)
                result   = self.client.create_and_post_order(args)
                order_id = result.get("orderID", "unknown")
                logging.info(f"MARKET SELL placed: {order_id}")
                return True, order_id
            except Exception as e:
                logging.warning(f"SELL attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return False, ""


# ==================== SEEN TRADES STORE ====================
class SeenTradesStore:
    """
    Persists every pos_key (wallet_addr_tokenId) we have ever attempted to copy.
    Loaded from disk on startup — survives bot restarts so we never re-copy
    a trade that already existed when the bot first saw it.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath
        self._seen: Set[str] = self._load()
        logging.info(f"SeenTradesStore loaded {len(self._seen)} historic trade keys from {filepath}")

    def _load(self) -> Set[str]:
        try:
            with open(self.filepath, "r") as f:
                data = json.load(f)
                return set(data) if isinstance(data, list) else set()
        except FileNotFoundError:
            return set()
        except Exception as e:
            logging.warning(f"Could not read seen trades file: {e}")
            return set()

    def _save(self):
        try:
            with open(self.filepath, "w") as f:
                json.dump(sorted(self._seen), f)
        except Exception as e:
            logging.warning(f"Could not save seen trades file: {e}")

    def is_seen(self, pos_key: str) -> bool:
        return pos_key in self._seen

    def mark_seen(self, pos_key: str):
        if pos_key not in self._seen:
            self._seen.add(pos_key)
            self._save()

    def snapshot_existing(self, pos_keys):
        """
        Called once on first scan per wallet to bulk-mark all currently open
        positions as already seen, so the bot skips them and only acts on
        trades that appear AFTER startup.
        """
        added = 0
        for key in pos_keys:
            if key not in self._seen:
                self._seen.add(key)
                added += 1
        if added:
            self._save()
            logging.info(f"Snapshot: marked {added} pre-existing trades as seen (will not copy)")


# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run      = dry_run
        self.balance      = RobustBalanceManager()
        self.positions:   Dict[str, Position]        = {}
        self.pending:     Dict[str, PendingLimitBuy] = {}
        self.executor     = PolymarketExecutor(dry_run)
        self.seen         = SeenTradesStore(SEEN_TRADES_FILE)
        self._first_scan: Set[str] = set()  # wallets not yet snapshotted this session

        logging.info(f"Multi-Wallet CopyTrader started | mode={'DRY RUN' if dry_run else 'LIVE'}")
        logging.info(
            f"Watching {len(WALLETS)} wallets | max positions={MAX_POSITIONS} | "
            f"limit cap=+{LIMIT_BUY_MAX_PREMIUM*100:.0f}% above source price | expiry={LIMIT_EXPIRY_SECONDS}s"
        )

    def get_orderbook_prices(self, token_id: str) -> Tuple[float, float]:
        """Returns (mid_price, best_ask). Either may be 0.0 on failure."""
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(
                    f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8
                )
                if r.status_code == 200:
                    data     = r.json()
                    bids     = data.get("bids", [])
                    asks     = data.get("asks", [])
                    best_bid = float(bids[0]["price"]) if bids else 0.0
                    best_ask = float(asks[0]["price"]) if asks else 0.0
                    mid      = (best_bid + best_ask) / 2 if best_bid and best_ask else best_bid or best_ask
                    return mid, best_ask
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    logging.warning(f"Orderbook fetch failed for {token_id[:12]}: {e}")
                time.sleep(RETRY_DELAY)
        return 0.0, 0.0

    def get_mid_price(self, token_id: str) -> float:
        mid, _ = self.get_orderbook_prices(token_id)
        return mid

    def _source_entry_price(self, pos: dict, best_ask: float, mid_price: float) -> float:
        """
        Determine the best limit price to use, in priority order:
        1. avgPrice / averagePrice from the position data (source wallet's actual avg entry)
        2. curPrice / currentPrice if present
        3. best_ask from the live orderbook (closest to what you'd pay right now)
        4. mid_price as last resort
        All results are capped at LIMIT_BUY_PRICE_CAP.
        """
        for key in ("avgPrice", "averagePrice", "avg_price", "average_price"):
            val = pos.get(key)
            if val:
                price = float(val)
                if 0 < price <= 1:
                    return price
        for key in ("curPrice", "currentPrice", "cur_price", "price"):
            val = pos.get(key)
            if val:
                price = float(val)
                if 0 < price <= 1:
                    return price
        if best_ask > 0:
            return best_ask
        return mid_price

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
        global peak_bankroll, bot_paused_until
        current = self.balance.get_balance()
        if current > peak_bankroll:
            peak_bankroll = current
        dd = (peak_bankroll - current) / peak_bankroll if peak_bankroll > 0 else 0
        if dd >= MAX_DRAWDOWN:
            if bot_paused_until is None or datetime.now() > bot_paused_until:
                bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
                logging.warning(
                    f"DRAWDOWN PROTECTION TRIGGERED ({dd*100:.1f}%) — paused {PAUSE_HOURS}h"
                )
            return True
        return False

    def _get_positions(self, wallet_addr: str) -> list | None:
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(
                    f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50",
                    timeout=12,
                )
                if resp.status_code == 200:
                    return resp.json()
            except Exception as e:
                logging.warning(f"Position fetch attempt {attempt+1} failed for {wallet_addr}: {e}")
                time.sleep(RETRY_DELAY)
        return None

    # ---- PENDING LIMIT ORDER MANAGEMENT ----
    def _process_pending_orders(self, source_token_ids_by_wallet: Dict[str, set]):
        """
        For each pending limit buy:
        - If filled → promote to open position
        - If source wallet no longer holds token → cancel and discard
        - If expired → cancel and retry with fresh mid-price
        """
        global current_bankroll
        for pos_key, pending in list(self.pending.items()):
            wallet_tokens = source_token_ids_by_wallet.get(pending.source_wallet, set())

            # Source wallet exited — cancel our pending order
            if pending.token_id not in wallet_tokens:
                logging.info(
                    f"Source exited before fill — cancelling pending {pending.question[:40]}"
                )
                self.executor.cancel_order(pending.order_id)
                del self.pending[pos_key]
                continue

            # Check if filled
            if self.executor.is_order_filled(pending.order_id):
                shares = pending.size_usd / pending.limit_price if pending.limit_price > 0 else 0
                self.positions[pos_key] = Position(
                    market_id     = pending.market_id,
                    question      = pending.question,
                    outcome       = pending.outcome,
                    token_id      = pending.token_id,
                    entry_price   = pending.limit_price,
                    size_usd      = pending.size_usd,
                    shares        = shares,
                    source_wallet = pending.source_wallet,
                    source_name   = pending.source_name,
                    order_id      = pending.order_id,
                )
                del self.pending[pos_key]
                logging.info(
                    f"LIMIT BUY FILLED → position open | {pending.question[:40]} "
                    f"@ {pending.limit_price:.4f}"
                )
                continue

            # Check expiry
            age = (datetime.now() - pending.placed_at).total_seconds()
            if age >= LIMIT_EXPIRY_SECONDS:
                logging.info(
                    f"Limit order expired after {age:.0f}s — cancelling and retrying "
                    f"{pending.question[:40]}"
                )
                self.executor.cancel_order(pending.order_id)
                del self.pending[pos_key]

                # Retry with fresh price
                mid_price, best_ask = self.get_orderbook_prices(pending.token_id)
                if mid_price <= 0:
                    continue
                # On retry, use best_ask as target (no source pos data available here)
                target_price = best_ask if best_ask > 0 else mid_price
                price_cap    = round(min(pending.limit_price * (1 + LIMIT_BUY_MAX_PREMIUM), 0.99), 4)
                if target_price > price_cap:
                    logging.info(
                        f"Retry skipped — price {target_price:.4f} > 20% cap {price_cap:.4f} "
                        f"for {pending.question[:40]}"
                    )
                    continue
                ok, order_id, limit_price = self.executor.place_limit_buy(
                    pending.token_id, pending.size_usd, target_price, pending.limit_price
                )
                if ok:
                    self.pending[pos_key] = PendingLimitBuy(
                        pos_key       = pos_key,
                        token_id      = pending.token_id,
                        market_id     = pending.market_id,
                        question      = pending.question,
                        outcome       = pending.outcome,
                        source_wallet = pending.source_wallet,
                        source_name   = pending.source_name,
                        limit_price   = limit_price,
                        size_usd      = pending.size_usd,
                        order_id      = order_id,
                    )
                    logging.info(
                        f"LIMIT BUY RETRIED | {pending.question[:40]} @ {limit_price:.4f}"
                    )

    async def scan_and_copy(self):
        global current_bankroll, bot_paused_until

        if bot_paused_until and datetime.now() < bot_paused_until:
            remaining = (bot_paused_until - datetime.now()).seconds // 60
            logging.info(f"Bot paused — {remaining} minutes remaining")
            return

        if self.check_drawdown():
            return

        current_bankroll = self.balance.get_balance()
        if current_bankroll is None:
            logging.error("Real balance unavailable — skipping this scan cycle")
            return
        logging.info(
            f"Scanning | bankroll=${current_bankroll:.2f} | "
            f"open={len(self.positions)} | pending={len(self.pending)}"
        )

        # Collect token IDs per wallet for pending order management
        source_token_ids_by_wallet: Dict[str, set] = {}

        for wallet_addr, config in WALLETS.items():
            raw = self._get_positions(wallet_addr)
            if raw is None:
                logging.warning(f"Skipping {config['name']} — could not fetch positions")
                continue

            source_token_ids = set()

            # Build source_token_ids first (needed for snapshot)
            for pos in raw:
                tid      = pos.get("asset", "")
                size_usd = float(pos.get("value", 0))
                if tid and size_usd >= 1.0:
                    source_token_ids.add(tid)

            # ---- FIRST SCAN SNAPSHOT ----
            # On the very first scan for this wallet each session, bulk-mark every
            # currently visible trade as already seen so we ONLY copy trades that
            # appear AFTER the bot started — never pre-existing ones.
            if wallet_addr not in self._first_scan:
                all_keys = {f"{wallet_addr}_{tid}" for tid in source_token_ids}
                self.seen.snapshot_existing(all_keys)
                self._first_scan.add(wallet_addr)
                logging.info(
                    f"First scan {config['name']} — "
                    f"{len(all_keys)} pre-existing trade(s) marked seen, will not copy"
                )

            # ---- BUY LOGIC (limit orders priced at source entry, capped at +20%) ----
            for pos in raw:
                token_id  = pos.get("asset", "")
                market_id = pos.get("market", "")
                question  = pos.get("title", "Unknown")
                outcome   = pos.get("outcome", "YES")
                size_usd  = float(pos.get("value", 0))

                if not token_id or size_usd < 1.0:
                    continue

                pos_key = f"{wallet_addr}_{token_id}"

                # Skip if seen before (pre-existing on startup OR previously copied)
                if self.seen.is_seen(pos_key):
                    continue

                # Skip if already in-flight or filled this session
                if pos_key in self.positions or pos_key in self.pending:
                    continue

                if len(self.positions) + len(self.pending) >= MAX_POSITIONS:
                    logging.info("Max positions reached — skipping new entries")
                    break

                mid_price, best_ask = self.get_orderbook_prices(token_id)
                if mid_price <= 0:
                    continue

                # Determine target price: source avg entry → best_ask → mid
                target_price = self._source_entry_price(pos, best_ask, mid_price)

                # Skip if market has already moved more than 20% above source entry
                price_cap = round(min(target_price * (1 + LIMIT_BUY_MAX_PREMIUM), 0.99), 4)
                if mid_price > price_cap:
                    logging.info(
                        f"Mid {mid_price:.4f} > 20% cap {price_cap:.4f} "
                        f"— skipping {question[:40]}"
                    )
                    # Mark seen so we don't recheck every poll
                    self.seen.mark_seen(pos_key)
                    continue

                risk_pct = self.get_risk_percent(target_price, config)
                my_size  = round(current_bankroll * risk_pct, 2)

                if my_size < 1.0:
                    logging.info(f"Size too small (${my_size:.2f}) — skipping {question[:40]}")
                    continue

                ok, order_id, limit_price = self.executor.place_limit_buy(
                    token_id, my_size, target_price, target_price
                )
                if ok:
                    # Persist immediately so restarts don't re-place this order
                    self.seen.mark_seen(pos_key)
                    self.pending[pos_key] = PendingLimitBuy(
                        pos_key       = pos_key,
                        token_id      = token_id,
                        market_id     = market_id,
                        question      = question,
                        outcome       = outcome,
                        source_wallet = wallet_addr,
                        source_name   = config["name"],
                        limit_price   = limit_price,
                        size_usd      = my_size,
                        order_id      = order_id,
                    )
                    logging.info(
                        f"LIMIT BUY PLACED {config['name']} | {question[:40]} | "
                        f"${my_size:.2f} @ {limit_price:.4f} "
                        f"(source≈{target_price:.4f} cap={price_cap:.4f} mid={mid_price:.4f})"
                    )

            source_token_ids_by_wallet[wallet_addr] = source_token_ids

            # ---- SELL LOGIC (market orders — instant exit) ----
            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr:
                    continue
                if position.token_id not in source_token_ids and position.status == "open":
                    exit_price, _ = self.get_orderbook_prices(position.token_id)
                    ok, _ = self.executor.place_sell(position.token_id, position.shares)
                    if ok:
                        pnl = (exit_price - position.entry_price) * position.shares
                        position.status     = "closed"
                        position.exit_price = exit_price
                        position.pnl        = pnl
                        logging.info(
                            f"MARKET SELL {position.question[:40]} | "
                            f"exit={exit_price:.4f} | pnl=${pnl:.2f}"
                        )
                        del self.positions[pos_key]

        # Process pending limit orders (fill checks, expiry, cancellations)
        self._process_pending_orders(source_token_ids_by_wallet)

    async def run(self):
        logging.info("Bot loop started")
        last_heartbeat = time.time()
        HEARTBEAT_INTERVAL = 300  # 5 minutes
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Main loop error: {e}")

            # Heartbeat every 5 minutes
            now = time.time()
            if now - last_heartbeat >= HEARTBEAT_INTERVAL:
                status = "PAUSED" if bot_paused_until and datetime.now() < bot_paused_until else "ACTIVE"
                logging.info(
                    f"💓 Heartbeat | Status: {status} | "
                    f"Bankroll: ${self.balance.cached_balance or 0:.2f} | "
                    f"Open: {len(self.positions)} | "
                    f"Pending: {len(self.pending)} | "
                    f"Watching: {len(WALLETS)} wallets"
                )
                last_heartbeat = now

            await asyncio.sleep(POLL_INTERVAL)


# ==================== ENTRY POINT ====================
async def main():
    global _bot_ref
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    bot = CopyTrader(dry_run=DRY_RUN)
    _bot_ref = bot

    # Confirm real balance before doing anything — raises if it can't
    starting_balance = bot.balance.fetch_with_retry(retries=5, delay=10)
    # Set peak to actual starting balance so drawdown is calculated correctly
    bot.balance.peak_balance = starting_balance
    global peak_bankroll
    peak_bankroll = starting_balance

    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
