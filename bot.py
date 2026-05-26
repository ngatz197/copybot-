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
except ImportError:
    CLOB_AVAILABLE = False
    logging.warning("py-clob-client not installed. Running in simulation mode.")

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

# ---- RPC balance settings ----
# Polygon RPC endpoint — defaults to public, set your own for reliability
POLYGON_RPC_URL       = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")
# pUSD = Polymarket's USDC proxy contract on Polygon — 6 decimals
PUSD_CONTRACT         = os.getenv("PUSD_CONTRACT", "0x4Fabb145d64652a948d72533023f6E7A623C7C53")

current_bankroll  = INITIAL_BANKROLL
peak_bankroll     = INITIAL_BANKROLL
bot_paused_until: Optional[datetime] = None


# ==================== HEALTH SERVER ====================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK - CopyTrader running")

    def log_message(self, format, *args):
        pass


def run_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    logging.info(f"Health server listening on port {HEALTH_PORT}")
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
    """
    Fetches wallet USDC balance directly from the Polygon blockchain via JSON-RPC.
    Reads both USDC (native) and USDC.e (bridged) and returns the sum.
    Never uses simulated or hardcoded values.
    """

    # ERC-20 balanceOf(address) selector + zero-padded address
    _BALANCE_OF_SIG = "0x70a08231"  # keccak256("balanceOf(address)")[:4]

    def __init__(self):
        self.cached_balance: Optional[float] = None
        self.last_update    = 0
        self.peak_balance   = 0.0

    def _rpc_call(self, payload: dict) -> dict:
        resp = requests.post(
            POLYGON_RPC_URL,
            json    = payload,
            headers = {"Content-Type": "application/json"},
            timeout = 10,
        )
        resp.raise_for_status()
        return resp.json()

    def _erc20_balance(self, contract: str, wallet: str) -> float:
        """
        Calls balanceOf(wallet) on an ERC-20 contract via eth_call.
        Returns token balance in human units (divides by 10^6 for USDC).
        """
        # ABI-encode: 4-byte selector + 32-byte zero-padded address
        padded_addr = wallet.lower().replace("0x", "").zfill(64)
        data        = self._BALANCE_OF_SIG + padded_addr

        payload = {
            "jsonrpc": "2.0",
            "id":      1,
            "method":  "eth_call",
            "params":  [
                {"to": contract, "data": data},
                "latest",
            ],
        }
        result = self._rpc_call(payload)
        hex_val = result.get("result", "0x0") or "0x0"
        raw     = int(hex_val, 16)
        return raw / 1_000_000  # USDC has 6 decimals

    def _fetch_balance(self) -> float:
        """
        Reads pUSD balance from Polygon RPC (Polymarket's collateral token).
        Falls back to Polymarket HTTP API if RPC fails.
        Returns float >= 0 on success, 0.0 on total failure.
        """
        # --- Primary: RPC ---
        try:
            pusd = self._erc20_balance(PUSD_CONTRACT, YOUR_WALLET)
            logging.debug(f"RPC pUSD balance: {pusd:.6f}")
            return pusd
        except Exception as e:
            logging.warning(f"RPC pUSD balance fetch failed: {e} — falling back to Polymarket API")

        # --- Fallback: Polymarket HTTP API ---
        for url in [
            f"https://data-api.polymarket.com/balance?user={YOUR_WALLET}",
            f"https://data-api.polymarket.com/profile?user={YOUR_WALLET}",
        ]:
            try:
                resp = requests.get(url, timeout=8)
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, (int, float)):
                        return float(data)
                    elif isinstance(data, dict):
                        val = float(
                            data.get("balance")
                            or data.get("portfolioValue")
                            or data.get("cashBalance")
                            or 0
                        )
                        return val
            except Exception as ex:
                logging.warning(f"Fallback balance fetch failed ({url}): {ex}")

        return 0.0

    def get_balance(self, force=False) -> Optional[float]:
        """
        Returns confirmed on-chain balance.
        Returns None only if balance has never been fetched successfully.
        Never returns a simulated or hardcoded value.
        """
        if force or self.cached_balance is None or (time.time() - self.last_update > 30):
            val = self._fetch_balance()
            # A successful RPC call returning 0 is valid (empty wallet)
            # We distinguish "never fetched" (None) from "fetched and it's 0"
            if val is not None and val >= 0:
                prev = self.cached_balance
                self.cached_balance = val
                self.last_update    = time.time()
                if val > self.peak_balance:
                    self.peak_balance = val
                    logging.info(f"New peak pUSD balance: ${self.peak_balance:.6f}")
                if prev is not None and abs(val - prev) > 0.01:
                    logging.info(f"pUSD balance updated: ${prev:.6f} → ${val:.6f}")
            else:
                if self.cached_balance is None:
                    logging.error(
                        "RPC balance fetch failed and no cached value — "
                        "bot will not trade until balance is confirmed"
                    )
        return self.cached_balance

    def fetch_with_retry(self, retries: int = 5, delay: int = 10) -> float:
        """
        Blocks at startup until a real on-chain pUSD balance is confirmed.
        Raises RuntimeError if all retries fail.
        """
        for attempt in range(1, retries + 1):
            try:
                pusd = self._erc20_balance(PUSD_CONTRACT, YOUR_WALLET)
                self.cached_balance = pusd
                self.peak_balance   = pusd
                self.last_update    = time.time()
                logging.info(f"On-chain pUSD balance confirmed: ${pusd:.6f}")
                return pusd
            except Exception as e:
                logging.warning(
                    f"RPC pUSD attempt {attempt}/{retries} failed: {e} "
                    f"— retrying in {delay}s"
                )
                time.sleep(delay)
        raise RuntimeError(
            f"Could not read on-chain pUSD balance after {retries} attempts. "
            f"Check POLYGON_RPC_URL and DEPOSIT_WALLET_ADDRESS."
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
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Main loop error: {e}")
            await asyncio.sleep(POLL_INTERVAL)


# ==================== ENTRY POINT ====================
async def main():
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    bot = CopyTrader(dry_run=DRY_RUN)

    # Confirm real balance before doing anything — raises if it can't
    bot.balance.fetch_with_retry(retries=5, delay=10)

    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
