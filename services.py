#!/usr/bin/env python3
import os
import json
import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Set, Tuple, Optional
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
import config as cfg

# ==================== CLOB V2 CLIENT ====================
try:
    from py_clob_client_v2 import (
        ClobClient,
        OrderArgs,
        MarketOrderArgs,
        OrderType,
        Side,
        ApiCreds,
        PartialCreateOrderOptions,
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

# Global environment overrides or fallbacks
YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
POLY_API_KEY     = os.getenv("POLY_API_KEY", "")
POLY_SECRET      = os.getenv("POLY_SECRET", "")
POLY_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")
DATABASE_URL     = os.getenv("DATABASE_URL", "")

INITIAL_BANKROLL      = 10.0
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "8"))
MAX_DRAWDOWN          = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT           = int(os.getenv("PORT", "8080"))
MAX_RETRIES           = 3
RETRY_DELAY           = 5

LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))
LIMIT_EXPIRY_SECONDS  = int(os.getenv("LIMIT_EXPIRY_SECONDS", "300"))
SEEN_TRADES_FILE      = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")
PUSD_CONTRACT_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

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
    current_price: float = 0.0

@dataclass
class PendingLimitBuy:
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

# ==================== SEEN TRADES STORE ====================
class SeenTradesStore:
    def __init__(self, filepath: str, db_url: str = ""):
        self.filepath = filepath
        self.db_url   = db_url
        self._seen: Set[str] = set()
        self._conn   = None

        if db_url and PSYCOPG2_AVAILABLE:
            self._init_postgres()
        else:
            self._load_file()

        logging.info(f"SeenTradesStore ready | backend={self.backend} | {len(self._seen)} historic keys loaded")

    def _init_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require")
            self._conn.autocommit = True
            with self._conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS seen_trades (
                        pos_key    TEXT PRIMARY KEY,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
            self._seen   = self._load_postgres()
            self.backend = "postgres"
            logging.info(f"Postgres connected — {len(self._seen)} seen keys loaded")
        except Exception as e:
            logging.error(f"Postgres init failed: {e} — falling back to local file")
            self._conn = None
            self._load_file()

    def _load_postgres(self) -> Set[str]:
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT pos_key FROM seen_trades")
                return {row[0] for row in cur.fetchall()}
        except Exception as e:
            logging.warning(f"Postgres load failed: {e}")
            return set()

    def _save_postgres(self, pos_key: str):
        try:
            with self._conn.cursor() as cur:
                cur.execute("INSERT INTO seen_trades (pos_key) VALUES (%s) ON CONFLICT DO NOTHING", (pos_key,))
        except Exception as e:
            logging.warning(f"Postgres save failed for {pos_key}: {e}")
            self._reconnect_postgres()

    def _save_postgres_many(self, keys):
        if not keys: return
        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur, "INSERT INTO seen_trades (pos_key) VALUES %s ON CONFLICT DO NOTHING", [(k,) for k in keys]
                )
        except Exception as e:
            logging.warning(f"Postgres bulk save failed: {e}")
            self._reconnect_postgres()

    def _reconnect_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require")
            self._conn.autocommit = True
            logging.info("Postgres reconnected")
        except Exception as e:
            logging.error(f"Postgres reconnect failed: {e}")

    def _load_file(self):
        try:
            with open(self.filepath, "r") as f:
                data = json.load(f)
                self._seen = set(data) if isinstance(data, list) else set()
        except FileNotFoundError:
            self._seen = set()
        except Exception as e:
            logging.warning(f"Could not read seen trades file: {e}")
            self._seen = set()
        self.backend = "local-file"

    def _save_file(self):
        try:
            with open(self.filepath, "w") as f:
                json.dump(sorted(self._seen), f)
        except Exception as e:
            logging.warning(f"Could not save seen trades file: {e}")

    def is_seen(self, pos_key: str) -> bool:
        return pos_key in self._seen

    def mark_seen(self, pos_key: str):
        if pos_key in self._seen: return
        self._seen.add(pos_key)
        if self._conn:
            self._save_postgres(pos_key)
        else:
            self._save_file()

    def snapshot_existing(self, pos_keys):
        new_keys = [k for k in pos_keys if k not in self._seen]
        if not new_keys: return
        for k in new_keys:
            self._seen.add(k)
        if self._conn:
            self._save_postgres_many(new_keys)
        else:
            self._save_file()
        logging.info(f"Snapshot: marked {len(new_keys)} pre-existing trades as seen")

    @property
    def is_empty(self) -> bool:
        return len(self._seen) == 0

# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    POLYGON_RPCS = [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com",
        "https://polygon.drpc.org",
    ]

    def __init__(self):
        self.cached_balance: Optional[float] = None
        self.last_update = 0
        self.peak_balance = 0.0

    def _fetch_balance(self) -> float:
        if not YOUR_WALLET:
            logging.error("DEPOSIT_WALLET_ADDRESS not set — cannot fetch balance")
            return 0.0
        padded = YOUR_WALLET.lower().replace("0x", "").zfill(64)
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": PUSD_CONTRACT_ADDRESS, "data": "0x70a08231" + padded}, "latest"],
            "id": 1,
        }
        for rpc in self.POLYGON_RPCS:
            try:
                resp = requests.post(rpc, json=payload, timeout=8)
                if resp.status_code == 200:
                    data = resp.json()
                    result = data.get("result", "0x0")
                    if result and result not in ("0x", "0x0"):
                        balance = int(result, 16) / 1_000_000
                        if balance > 0: return balance
            except Exception as e:
                logging.warning(f"RPC balance fetch failed ({rpc}): {e}")
                continue
        return 0.0

    def get_balance(self, force=False) -> Optional[float]:
        if force or self.cached_balance is None or (time.time() - self.last_update > 30):
            real = self._fetch_balance()
            if real > 0:
                self.cached_balance = real
                self.last_update = time.time()
                if real > self.peak_balance:
                    self.peak_balance = real
                    cfg.peak_bankroll = real
                    logging.info(f"New peak balance: ${self.peak_balance:.2f}")
            else:
                if self.cached_balance is None:
                    logging.error("Could not fetch real pUSD balance — bot will not trade.")
        return self.cached_balance

    def fetch_with_retry(self, retries: int = 5, delay: int = 10) -> float:
        for attempt in range(1, retries + 1):
            val = self._fetch_balance()
            if val > 0:
                self.cached_balance = val
                self.peak_balance = val
                self.last_update = time.time()
                logging.info(f"Real pUSD balance confirmed: ${val:.2f}")
                return val
            logging.warning(f"Balance fetch attempt {attempt}/{retries} returned 0 — retrying...")
            time.sleep(delay)
        raise RuntimeError(f"Could not fetch real pUSD balance after {retries} attempts.")

    def check_drawdown(self) -> Tuple[bool, float]:
        current = self.get_balance()
        if current is None or self.peak_balance == 0: return False, 0.0
        dd = (self.peak_balance - current) / self.peak_balance
        return dd >= MAX_DRAWDOWN, dd

# ==================== EXECUTOR (V2) ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client = None
        if not dry_run and CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(
                    api_key = POLY_API_KEY,
                    api_secret = POLY_SECRET,
                    api_passphrase = POLY_PASSPHRASE,
                )
                self.client = ClobClient(
                    host = "https://clob.polymarket.com",
                    chain_id = 137,
                    key = YOUR_PRIVATE_KEY,
                    creds = creds,
                )
                logging.info("ClobClient V2 initialised — LIVE mode")
            except Exception as e:
                logging.error(f"ClobClient V2 init failed: {e}")
                self.client = None

    def place_limit_buy(self, token_id: str, amount_usd: float, limit_price: float) -> Tuple[bool, str, float]:
        shares = round(amount_usd / limit_price, 4)
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] LIMIT BUY {shares:.4f} shares @ {limit_price:.4f} (${amount_usd:.2f})")
            return True, "dry-run-limit-buy", limit_price
        for attempt in range(MAX_RETRIES):
            try:
                result = self.client.create_and_post_order(
                    order_args = OrderArgs(token_id=token_id, price=limit_price, size=shares, side=Side.BUY),
                    options = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.GTC,
                )
                order_id = result.get("orderID", result.get("id", "unknown"))
                logging.info(f"LIMIT BUY placed (V2): {order_id} | {shares:.4f} shares @ {limit_price:.4f}")
                return True, order_id, limit_price
            except Exception as e:
                logging.warning(f"LIMIT BUY attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return False, "", limit_price

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

    def is_order_filled(self, order_id: str) -> bool:
        if self.dry_run or self.client is None: return True
        try:
            order = self.client.get_order(order_id)
            status = order.get("status", "").lower()
            return status in ("matched", "filled")
        except Exception as e:
            logging.warning(f"Could not check order status for {order_id}: {e}")
            return False

    def place_sell(self, token_id: str, shares: float) -> Tuple[bool, str]:
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] MARKET SELL {shares:.4f} shares")
            return True, "dry-run-sell"
        for attempt in range(MAX_RETRIES):
            try:
                result = self.client.create_and_post_market_order(
                    order_args = MarketOrderArgs(token_id=token_id, amount=shares, side=Side.SELL),
                    options = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.FOK,
                )
                order_id = result.get("orderID", result.get("id", "unknown"))
                logging.info(f"MARKET SELL placed (V2): {order_id}")
                return True, order_id
            except Exception as e:
                logging.warning(f"SELL attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return False, ""

# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.balance = RobustBalanceManager()
        self.positions: Dict[str, Position] = {}
        self.pending: Dict[str, PendingLimitBuy] = {}
        self.executor = PolymarketExecutor(dry_run)
        self.seen = SeenTradesStore(SEEN_TRADES_FILE, DATABASE_URL)
        self._first_scan_done: Set[str] = set()
        self.closed_positions: list = []
        logging.info(f"Multi-Wallet CopyTrader V2 started | mode={'DRY RUN' if dry_run else 'LIVE'}")

    def _get_positions_sync(self, wallet_addr: str) -> Optional[list]:
        """Blocking fetch — runs inside a thread via _fetch_all_wallets."""
        url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50"
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(url, timeout=12)
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 404:
                    return []
                else:
                    logging.warning(f"[_get_positions] HTTP {resp.status_code} for {wallet_addr[:10]}")
            except Exception as e:
                logging.warning(f"[_get_positions] Attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return None

    async def _fetch_all_wallets(self) -> Dict[str, Optional[list]]:
        """Fetch all wallet positions simultaneously using a thread pool."""
        loop = asyncio.get_event_loop()
        wallet_addrs = list(cfg.WALLETS.keys())

        tasks = [
            loop.run_in_executor(None, self._get_positions_sync, addr)
            for addr in wallet_addrs
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        out = {}
        for addr, result in zip(wallet_addrs, results):
            if isinstance(result, Exception):
                logging.warning(f"[_fetch_all_wallets] Exception for {addr[:10]}: {result}")
                out[addr] = None
            else:
                out[addr] = result
        return out

    def get_orderbook_prices(self, token_id: str) -> Tuple[float, float]:
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8)
                if r.status_code == 200:
                    data = r.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    best_bid = float(bids[0]["price"]) if bids else 0.0
                    best_ask = float(asks[0]["price"]) if asks else 0.0
                    mid = (best_bid + best_ask) / 2 if best_bid and best_ask else (best_bid or best_ask or 0.50)
                    return best_ask, mid
            except Exception as e:
                logging.warning(f"Orderbook request error: {e}")
                time.sleep(1)
        return 0.0, 0.50

    def get_market_question(self, market_id: str) -> str:
        if not market_id or market_id == "unknown": return "Polymarket Asset"
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(f"https://clob.polymarket.com/markets/{market_id}", timeout=8)
                if r.status_code == 200:
                    return r.json().get("question", "Polymarket Asset")
            except Exception:
                time.sleep(1)
        return "Polymarket Asset"

    def clean_expired_limit_orders(self):
        now = datetime.now()
        for k, p in list(self.pending.items()):
            if (now - p.placed_at).total_seconds() >= LIMIT_EXPIRY_SECONDS:
                logging.info(f"[EXPIRY] Pending buy limit order expired for {p.source_name} -> Cancelling...")
                if self.executor.cancel_order(p.order_id):
                    del self.pending[k]

    def process_pending_fills(self):
        for k, p in list(self.pending.items()):
            if self.executor.is_order_filled(p.order_id):
                logging.info(f"✨ [FILL] Limit Order FILLED for {p.source_name} on {p.outcome}")
                self.positions[k] = Position(
                    market_id=p.market_id, question=p.question, outcome=p.outcome,
                    token_id=p.token_id, entry_price=p.limit_price, size_usd=p.size_usd,
                    shares=round(p.size_usd / p.limit_price, 4), source_wallet=p.source_wallet,
                    source_name=p.source_name, order_id=p.order_id, current_price=p.limit_price
                )
                del self.pending[k]

    async def scan_and_copy(self):
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until: return

        is_broken, dd_pct = self.balance.check_drawdown()
        if is_broken:
            logging.critical(f"🛑 CRITICAL DRAWDOWN TRIGGERED ({dd_pct*100:.1f}%)! Locking operations.")
            cfg.bot_paused_until = datetime.now() + timedelta(hours=48)
            return

        current_bal = self.balance.get_balance()
        if current_bal is None: return

        self.clean_expired_limit_orders()
        self.process_pending_fills()

        logging.info(f"Scan Run | Net Assets: ${current_bal:.2f} pUSD | Active Orders: {len(self.positions)} | Open Limits: {len(self.pending)}")
        source_token_ids_by_wallet = {}

        # ---- Fetch all 4 wallets simultaneously ----
        all_wallet_data = await self._fetch_all_wallets()

        for wallet_addr, config in cfg.WALLETS.items():
            raw = all_wallet_data.get(wallet_addr)
            if raw is None:
                logging.warning(f"[{config['name']}] Failed to fetch positions from API.")
                continue

            source_token_ids = {
                pos.get("asset") for pos in raw
                if pos.get("asset") and float(pos.get("size", pos.get("shares", 0))) > 0
            }

            logging.info(f"[{config['name']}] {len(raw)} position(s) from API, {len(source_token_ids)} with active tokens")

            if wallet_addr not in self._first_scan_done:
                if config.get("copy_mode") == "new_only":
                    pre_existing = []
                    for pos in raw:
                        asset_id = pos.get("asset")
                        side = pos.get("side", "YES").upper()
                        if asset_id and float(pos.get("size", pos.get("shares", 0))) > 0:
                            pre_existing.append(f"{wallet_addr.lower()}_{asset_id}_{side}")
                    self.seen.snapshot_existing(pre_existing)
                self._first_scan_done.add(wallet_addr)

            for pos in raw:
                token_id  = pos.get("asset")
                shares    = float(pos.get("size", pos.get("shares", 0)))
                side      = pos.get("side", "YES").upper()
                market_id = pos.get("conditionId", "unknown")

                if not token_id or shares <= 0: continue

                pos_key = f"{wallet_addr.lower()}_{token_id}_{side}"
                if self.seen.is_seen(pos_key) or pos_key in self.pending:
                    continue

                if len(self.positions) >= MAX_POSITIONS:
                    logging.warning(f"Position limit max reached ({MAX_POSITIONS}) — Skipping buy trade execution.")
                    continue

                if config.get("risk_type") == "fixed":
                    my_size = cfg.compounding_bankroll * config.get("fixed_risk", 0.025)
                else:
                    my_size = float(pos.get("initialValue", pos.get("value", 2.0)))

                if my_size < 1.0 and not config.get("copy_sub_dollar", False):
                    logging.info(f"[{config['name']}] Sub-dollar size (${my_size:.2f}) rejected.")
                    continue

                best_ask, mid_price = self.get_orderbook_prices(token_id)
                if best_ask <= 0:
                    actual_price = mid_price
                else:
                    premium = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
                    actual_price = min(best_ask, mid_price * (1.0 + premium))

                if actual_price <= 0 or actual_price >= 1.0:
                    logging.error(f"Invalid market conversion pricing metrics: {actual_price}")
                    continue

                question_str = self.get_market_question(market_id)
                logging.info(f"🛒 [SIGNAL DETECTED] Wallet {config['name']} bought {side} on '{question_str[:40]}'")

                ok, order_id, _ = self.executor.place_limit_buy(token_id, my_size, actual_price)
                if ok:
                    self.seen.mark_seen(pos_key)
                    self.pending[pos_key] = PendingLimitBuy(
                        pos_key=pos_key, token_id=token_id, market_id=market_id,
                        question=question_str, outcome=side, source_wallet=wallet_addr,
                        source_name=config["name"], limit_price=actual_price, size_usd=my_size, order_id=order_id
                    )

            source_token_ids_by_wallet[wallet_addr] = source_token_ids

            cur_price_map = {
                pos.get("asset"): float(pos.get("curPrice", 0))
                for pos in raw
                if pos.get("asset") and float(pos.get("curPrice", 0)) > 0
            }
            for _pk, _pos in self.positions.items():
                if _pos.source_wallet == wallet_addr and _pos.token_id in cur_price_map:
                    _pos.current_price = cur_price_map[_pos.token_id]

            # Evaluate Exits / Auto Sells
            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr: continue
                if position.token_id not in source_token_ids and position.status == "open":
                    logging.info(f"📉 [EXIT DETECTED] Source wallet closed out position. Syncing asset sell execution...")
                    exit_price, _ = self.get_orderbook_prices(position.token_id)
                    ok, _ = self.executor.place_sell(position.token_id, position.shares)
                    if ok:
                        pnl = (exit_price - position.entry_price) * position.shares
                        position.status, position.exit_price, position.pnl = "closed", exit_price, pnl
                        if pnl > 0:
                            cfg.compounding_bankroll += pnl * cfg.COMPOUNDING_RATE
                        self.closed_positions.append(position)
                        del self.positions[pos_key]

# ==================== WEB DASHBOARD INFRASTRUCTURE ====================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>CopyTrader Dashboard</title>
    <meta http-equiv="refresh" content="15">
    <style>
        *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: #0d0d0f; color: #e2e8f0; min-height: 100vh; padding: 24px 16px; }}
        .page {{ max-width: 1100px; margin: 0 auto; }}
        .header {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 28px; flex-wrap: wrap; gap: 8px; }}
        .header-title {{ font-size: 1.25rem; font-weight: 700; color: #f8fafc; letter-spacing: -0.3px; }}
        .header-title span {{ color: #6ee7b7; }}
        .badge {{ font-size: 0.72rem; font-weight: 600; padding: 3px 10px; border-radius: 999px; letter-spacing: 0.4px; text-transform: uppercase; }}
        .badge-live  {{ background: #064e3b; color: #6ee7b7; border: 1px solid #065f46; }}
        .badge-dry   {{ background: #1e1b4b; color: #a5b4fc; border: 1px solid #312e81; }}
        .badge-paused{{ background: #450a0a; color: #fca5a5; border: 1px solid #7f1d1d; }}
        .timestamp   {{ font-size: 0.75rem; color: #64748b; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin-bottom: 24px; }}
        .stat-card {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px; padding: 18px 20px; }}
        .stat-label {{ font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.6px; color: #64748b; margin-bottom: 6px; }}
        .stat-value {{ font-size: 1.6rem; font-weight: 700; color: #f1f5f9; line-height: 1; }}
        .stat-sub {{ font-size: 0.75rem; color: #475569; margin-top: 5px; }}
        .pos  {{ color: #34d399; }}
        .neg  {{ color: #f87171; }}
        .neu  {{ color: #94a3b8; }}
        .section {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px; margin-bottom: 20px; overflow: hidden; }}
        .section-header {{ display: flex; align-items: center; justify-content: space-between; padding: 14px 20px; border-bottom: 1px solid #1e2230; }}
        .section-title {{ font-size: 0.85rem; font-weight: 700; color: #cbd5e1; text-transform: uppercase; letter-spacing: 0.5px; }}
        .count-pill {{ font-size: 0.72rem; font-weight: 700; background: #1e2230; color: #94a3b8; border-radius: 999px; padding: 2px 10px; }}
        .tbl-wrap {{ overflow-x: auto; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.82rem; }}
        thead th {{ padding: 10px 16px; text-align: left; font-size: 0.70rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; color: #475569; background: #13151a; white-space: nowrap; }}
        tbody tr {{ border-top: 1px solid #1a1d26; transition: background 0.15s; }}
        tbody tr:hover {{ background: #1c1f28; }}
        tbody td {{ padding: 12px 16px; color: #cbd5e1; vertical-align: middle; }}
        .market-name {{ font-weight: 500; color: #e2e8f0; max-width: 300px; }}
        .outcome-pill {{ display: inline-block; font-size: 0.68rem; font-weight: 700; padding: 2px 8px; border-radius: 999px; text-transform: uppercase; letter-spacing: 0.3px; }}
        .outcome-yes {{ background: #064e3b; color: #6ee7b7; }}
        .outcome-no  {{ background: #450a0a; color: #fca5a5; }}
        .source-tag {{ font-size: 0.70rem; font-weight: 600; color: #818cf8; background: #1e1b4b; padding: 2px 8px; border-radius: 999px; }}
        .price-mono {{ font-family: 'Courier New', monospace; font-size: 0.80rem; }}
        .pnl-cell {{ font-weight: 700; font-size: 0.83rem; white-space: nowrap; }}
        .empty {{ padding: 32px 20px; text-align: center; color: #334155; font-size: 0.85rem; }}
        .empty-icon {{ font-size: 1.8rem; margin-bottom: 8px; }}
    </style>
</head>
<body>
<div class="page">
    <div class="header">
        <div>
            <div class="header-title">🤖 Poly<span>CopyTrader</span></div>
            <div class="timestamp">Updated {last_updated} &nbsp;·&nbsp; Auto-refresh 15s</div>
        </div>
        <div style="display:flex;gap:8px;align-items:center;">
            <span class="badge {mode_badge}">{mode_label}</span>
            <span class="badge {status_badge}">{status_label}</span>
        </div>
    </div>
    <div class="stats">
        <div class="stat-card">
            <div class="stat-label">Total Balance</div>
            <div class="stat-value">${balance:.2f}</div>
            <div class="stat-sub">pUSD &nbsp;·&nbsp; Peak ${peak:.2f}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Compounding Bankroll</div>
            <div class="stat-value {comp_cls}">${comp_bankroll:.2f}</div>
            <div class="stat-sub">Sizing base &nbsp;·&nbsp; Rate {comp_rate:.0f}%</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Total PnL</div>
            <div class="stat-value {total_pnl_cls}">{total_pnl_sign}${total_pnl_abs}</div>
            <div class="stat-sub">Realised + Unrealised</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Unrealised</div>
            <div class="stat-value {unreal_cls}">{unreal_sign}${unreal_abs}</div>
            <div class="stat-sub">{open_count} open position(s)</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Realised</div>
            <div class="stat-value {real_cls}">{real_sign}${real_abs}</div>
            <div class="stat-sub">{closed_count} closed trade(s)</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Drawdown</div>
            <div class="stat-value {dd_cls}">{drawdown:.1f}%</div>
            <div class="stat-sub">Max {max_dd:.0f}%</div>
        </div>
    </div>
    <div class="section">
        <div class="section-header"><span class="section-title">Open Positions</span><span class="count-pill">{open_count}</span></div>
        {positions_block}
    </div>
    <div class="section">
        <div class="section-header"><span class="section-title">Closed Trades</span><span class="count-pill">{closed_count}</span></div>
        {closed_block}
    </div>
</div>
</body>
</html>
"""

def build_dashboard(bot) -> dict:
    def _sign(v): return "+" if v > 0 else ("-" if v < 0 else "")
    def _cls(v):  return "pos" if v > 0 else ("neg" if v < 0 else "neu")

    bankroll   = bot.balance.cached_balance or 0.0
    drawdown   = ((cfg.peak_bankroll - bankroll) / cfg.peak_bankroll * 100) if cfg.peak_bankroll > 0 else 0.0
    is_paused  = bool(cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until)

    status_label = "Paused" if is_paused else "Running"
    status_badge = "badge-paused" if is_paused else "badge-live"
    mode_label   = "Dry Run" if bot.dry_run else "Live"
    mode_badge   = "badge-dry" if bot.dry_run else "badge-live"

    unrealised = 0.0
    pos_rows   = ""
    for p in bot.positions.values():
        mid    = p.current_price if p.current_price > 0 else p.entry_price
        unreal = (mid - p.entry_price) * p.shares
        unrealised += unreal

        outcome_cls = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        pnl_cls     = _cls(unreal)
        pnl_fmt     = ".4f" if abs(unreal) < 0.005 else ".2f"
        pnl_str     = f"{_sign(unreal)}${abs(unreal):{pnl_fmt}}"
        cur_str     = f"{mid:.3f}" if p.current_price > 0 else "—"
        shares_str  = f"{p.shares:.4f}"

        pos_rows += f"""
        <tr>
            <td><span class="source-tag">{p.source_name}</span></td>
            <td class="market-name">{p.question[:60]}</td>
            <td><span class="outcome-pill {outcome_cls}">{p.outcome}</span></td>
            <td>${p.size_usd:.2f}<br><span style="font-size:0.70rem;color:#475569;">{shares_str} shares</span></td>
            <td class="price-mono">{p.entry_price:.3f}</td>
            <td class="price-mono">{cur_str}</td>
            <td class="pnl-cell {pnl_cls}">{pnl_str}</td>
        </tr>"""

    if pos_rows:
        positions_block = f"""
        <div class="tbl-wrap">
        <table>
            <thead><tr><th>Source</th><th>Market</th><th>Side</th><th>Size</th><th>Entry</th><th>Current</th><th>Unreal PnL</th></tr></thead>
            <tbody>{pos_rows}</tbody>
        </table>
        </div>"""
    else:
        positions_block = '<div class="empty"><div class="empty-icon">📭</div>No open positions</div>'

    closed_list = getattr(bot, "closed_positions", [])
    realised    = sum(p.pnl for p in closed_list)
    closed_rows = ""
    for p in reversed(closed_list):
        outcome_cls = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        pnl_cls     = _cls(p.pnl)
        pnl_str     = f"{_sign(p.pnl)}${abs(p.pnl):.2f}"
        closed_rows += f"""
        <tr>
            <td><span class="source-tag">{p.source_name}</span></td>
            <td class="market-name">{p.question[:60]}</td>
            <td><span class="outcome-pill {outcome_cls}">{p.outcome}</span></td>
            <td class="price-mono">{p.entry_price:.3f}</td>
            <td class="price-mono">{p.exit_price:.3f}</td>
            <td class="pnl-cell {pnl_cls}">{pnl_str}</td>
        </tr>"""

    if closed_rows:
        closed_block = f"""
        <div class="tbl-wrap">
        <table>
            <thead><tr><th>Source</th><th>Market</th><th>Side</th><th>Entry</th><th>Exit</th><th>Realised PnL</th></tr></thead>
            <tbody>{closed_rows}</tbody>
        </table>
        </div>"""
    else:
        closed_block = '<div class="empty"><div class="empty-icon">📋</div>No closed trades yet</div>'

    total_pnl = realised + unrealised
    def _fmt(v): return f"{abs(v):.4f}" if abs(v) < 0.005 else f"{abs(v):.2f}"
    comp_delta = cfg.compounding_bankroll - (bot.balance.peak_balance or INITIAL_BANKROLL)

    return {
        "last_updated":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode_label":      mode_label,
        "mode_badge":      mode_badge,
        "status_label":    status_label,
        "status_badge":    status_badge,
        "balance":         bankroll,
        "peak":            cfg.peak_bankroll,
        "drawdown":        drawdown,
        "dd_cls":          "neg" if drawdown > 10 else ("neu" if drawdown > 5 else "pos"),
        "max_dd":          MAX_DRAWDOWN * 100,
        "comp_bankroll":   cfg.compounding_bankroll,
        "comp_cls":        _cls(comp_delta),
        "comp_rate":       cfg.COMPOUNDING_RATE * 100,
        "total_pnl_cls":   _cls(total_pnl),
        "total_pnl_sign":  _sign(total_pnl),
        "total_pnl_abs":   _fmt(total_pnl),
        "unreal_cls":      _cls(unrealised),
        "unreal_sign":     _sign(unrealised),
        "unreal_abs":      _fmt(unrealised),
        "real_cls":        _cls(realised),
        "real_sign":       _sign(realised),
        "real_abs":        _fmt(realised),
        "open_count":      len(bot.positions),
        "closed_count":    len(closed_list),
        "positions_block": positions_block,
        "closed_block":    closed_block,
    }

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" and cfg._bot_ref:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            try:
                data = build_dashboard(cfg._bot_ref)
                html = HTML_TEMPLATE.format(**data)
                self.wfile.write(html.encode())
            except Exception:
                self.wfile.write(b"<h1>Dashboard loading...</h1>")
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK - CopyTrader V2 running")

    def log_message(self, format, *args): pass

def run_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    logging.info(f"🌐 Dashboard live at http://0.0.0.0:{HEALTH_PORT}")
    server.serve_forever()
