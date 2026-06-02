#!/usr/bin/env python3
import os
import json
import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Set, Tuple, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
import config as cfg

# ==================== OPTIONAL DEPENDENCIES ====================
try:
    from py_clob_client_v2 import (
        ClobClient, OrderArgs, MarketOrderArgs,
        OrderType, Side, ApiCreds, PartialCreateOrderOptions,
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

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    logging.warning("websockets not installed — WS listener disabled. Run: pip install websockets")

# ==================== ENVIRONMENT / CONSTANTS ====================
YOUR_PRIVATE_KEY      = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET           = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
POLY_API_KEY          = os.getenv("POLY_API_KEY", "")
POLY_SECRET           = os.getenv("POLY_SECRET", "")
POLY_PASSPHRASE       = os.getenv("POLY_PASSPHRASE", "")
DATABASE_URL          = os.getenv("DATABASE_URL", "")

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
    signal_source: str   = "rest"   # "ws" | "rest"

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
    signal_source: str      = "rest"   # "ws" | "rest"
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
                cur.execute(
                    "INSERT INTO seen_trades (pos_key) VALUES (%s) ON CONFLICT DO NOTHING",
                    (pos_key,)
                )
        except Exception as e:
            logging.warning(f"Postgres save failed for {pos_key}: {e}")
            self._reconnect_postgres()

    def _save_postgres_many(self, keys):
        if not keys: return
        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO seen_trades (pos_key) VALUES %s ON CONFLICT DO NOTHING",
                    [(k,) for k in keys]
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
        padded  = YOUR_WALLET.lower().replace("0x", "").zfill(64)
        payload = {
            "jsonrpc": "2.0",
            "method":  "eth_call",
            "params":  [{"to": PUSD_CONTRACT_ADDRESS, "data": "0x70a08231" + padded}, "latest"],
            "id":      1,
        }
        for rpc in self.POLYGON_RPCS:
            try:
                resp = requests.post(rpc, json=payload, timeout=8)
                if resp.status_code == 200:
                    result = resp.json().get("result", "0x0")
                    if result and result not in ("0x", "0x0"):
                        balance = int(result, 16) / 1_000_000
                        if balance > 0:
                            return balance
            except Exception as e:
                logging.warning(f"RPC balance fetch failed ({rpc}): {e}")
        return 0.0

    def get_balance(self, force=False) -> Optional[float]:
        if force or self.cached_balance is None or (time.time() - self.last_update > 30):
            real = self._fetch_balance()
            if real > 0:
                self.cached_balance = real
                self.last_update    = time.time()
                if real > self.peak_balance:
                    self.peak_balance  = real
                    cfg.peak_bankroll  = real
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
                self.peak_balance   = val
                self.last_update    = time.time()
                logging.info(f"Real pUSD balance confirmed: ${val:.2f}")
                return val
            logging.warning(f"Balance fetch attempt {attempt}/{retries} returned 0 — retrying...")
            time.sleep(delay)
        raise RuntimeError(f"Could not fetch real pUSD balance after {retries} attempts.")

    def check_drawdown(self) -> Tuple[bool, float]:
        current = self.get_balance()
        if current is None or self.peak_balance == 0:
            return False, 0.0
        dd = (self.peak_balance - current) / self.peak_balance
        return dd >= MAX_DRAWDOWN, dd

# ==================== EXECUTOR (V2) ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client  = None
        if not dry_run and CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(
                    api_key        = POLY_API_KEY,
                    api_secret     = POLY_SECRET,
                    api_passphrase = POLY_PASSPHRASE,
                )
                self.client = ClobClient(
                    host     = "https://clob.polymarket.com",
                    chain_id = 137,
                    key      = YOUR_PRIVATE_KEY,
                    creds    = creds,
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
                result   = self.client.create_and_post_order(
                    order_args = OrderArgs(token_id=token_id, price=limit_price, size=shares, side=Side.BUY),
                    options    = PartialCreateOrderOptions(tick_size="0.01"),
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
        if self.dry_run or self.client is None:
            return True
        try:
            status = self.client.get_order(order_id).get("status", "").lower()
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
                result   = self.client.create_and_post_market_order(
                    order_args = MarketOrderArgs(token_id=token_id, amount=shares, side=Side.SELL),
                    options    = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.FOK,
                )
                order_id = result.get("orderID", result.get("id", "unknown"))
                logging.info(f"MARKET SELL placed (V2): {order_id}")
                return True, order_id
            except Exception as e:
                logging.warning(f"SELL attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return False, ""

# ==================== WEBSOCKET LISTENER ====================
class PolymarketWSListener:
    """
    Real-time trade signal layer (~0–2 s latency).

    Subscribes to Polymarket's CLOB WebSocket on both the 'market' (price) and
    'trade' (fills) channels.  When a confirmed fill arrives, the registered
    on_trade_callback coroutine is called immediately — outside the poll loop —
    so copy orders are placed without waiting for the next REST scan.

    Price updates are enqueued in ws_price_queue for the poll loop to drain each
    cycle, keeping dashboard P&L fresh between REST polls.

    Reconnects automatically with exponential back-off on any disconnect.
    Supports live incremental token subscription without a full reconnect.

    Requires: pip install websockets>=12
    """

    WS_URL         = "wss://ws-subscriptions-clob.polymarket.com/ws/"
    PING_INTERVAL  = 20
    RECONNECT_BASE =  2
    RECONNECT_MAX  = 60

    def __init__(
        self,
        token_ids:       Set[str],
        ws_price_queue:  asyncio.Queue,
        on_trade_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
    ):
        self.token_ids          = token_ids          # mutable — shared with CopyTrader
        self.ws_price_queue     = ws_price_queue
        self.on_trade_callback  = on_trade_callback  # set by CopyTrader after init
        self._running           = False
        self._ws                = None
        self._subscribed: Set[str] = set()

    # ---- public API ----

    async def subscribe_token(self, token_id: str):
        """Register a new token. If connected, sends incremental subscribe immediately."""
        if token_id in self._subscribed:
            return
        self.token_ids.add(token_id)
        if self._ws is not None:
            try:
                await self._send_subscribe(self._ws, {token_id})
                self._subscribed.add(token_id)
                logging.info(f"[WS] Live-subscribed token {token_id[:12]}…")
            except Exception as e:
                logging.warning(f"[WS] Live subscribe failed for {token_id[:12]}: {e}")

    async def run(self):
        """Entry point — call with asyncio.create_task()."""
        if not WEBSOCKETS_AVAILABLE:
            logging.warning("[WS] websockets not installed — listener inactive.")
            return
        self._running = True
        delay = self.RECONNECT_BASE
        while self._running:
            try:
                await self._connect_and_listen()
                delay = self.RECONNECT_BASE
            except Exception as e:
                logging.warning(f"[WS] Disconnected: {e} — reconnecting in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.RECONNECT_MAX)

    def stop(self):
        self._running = False

    # ---- internals ----

    async def _connect_and_listen(self):
        logging.info(f"[WS] Connecting to {self.WS_URL} …")
        async with websockets.connect(
            self.WS_URL,
            ping_interval = self.PING_INTERVAL,
            ping_timeout  = 30,
            close_timeout = 10,
        ) as ws:
            self._ws = ws
            self._subscribed.clear()
            logging.info("[WS] Connected ✅")

            if self.token_ids:
                await self._send_subscribe(ws, self.token_ids)
                self._subscribed.update(self.token_ids)
            else:
                logging.info("[WS] No token_ids yet — awaiting first trade signal.")

            async for raw in ws:
                if not self._running:
                    break
                try:
                    await self._handle_message(raw)
                except Exception as e:
                    logging.debug(f"[WS] Message parse error: {e}")
        self._ws = None

    async def _send_subscribe(self, ws, token_ids: Set[str]):
        for channel in ("market", "trade"):
            payload = {
                "type":      "subscribe",
                "channel":   channel,
                "asset_ids": list(token_ids),
            }
            await ws.send(json.dumps(payload))
        logging.info(f"[WS] Subscribed {len(token_ids)} token(s) on market+trade channels")

    async def _handle_message(self, raw: str):
        """
        Parse a WS message and route it:
          - price/book events  → ws_price_queue  (drained each poll cycle)
          - trade/fill events  → on_trade_callback (fires immediately, no poll wait)
        """
        try:
            events = json.loads(raw)
        except json.JSONDecodeError:
            return

        if not isinstance(events, list):
            events = [events]

        for ev in events:
            ev_type = ev.get("event_type") or ev.get("type") or ""

            # ---- price / book update → queue for poll-cycle drain ----
            if ev_type in ("price_change", "book", "last_trade_price"):
                token_id = ev.get("asset_id") or ev.get("market") or ""
                price    = (
                    float(ev.get("price", 0))
                    or float(ev.get("mid_price", 0))
                    or float(ev.get("last_trade_price", 0))
                )
                if token_id and price:
                    try:
                        self.ws_price_queue.put_nowait({
                            "kind":     "price_update",
                            "token_id": token_id,
                            "price":    price,
                        })
                    except asyncio.QueueFull:
                        # drop oldest, insert newest
                        try:
                            self.ws_price_queue.get_nowait()
                            self.ws_price_queue.put_nowait({
                                "kind": "price_update", "token_id": token_id, "price": price,
                            })
                        except Exception:
                            pass

            # ---- confirmed fill → immediate callback, bypass poll loop ----
            elif ev_type in ("trade", "order_filled"):
                token_id   = ev.get("asset_id") or ev.get("market") or ""
                price      = float(ev.get("price", 0))
                size       = float(ev.get("size", 0))
                side       = (ev.get("side") or ev.get("outcome") or "YES").upper()
                maker_addr = (ev.get("maker_address") or ev.get("maker") or "").lower()
                taker_addr = (ev.get("taker_address") or ev.get("taker") or "").lower()

                if token_id and price and self.on_trade_callback:
                    await self.on_trade_callback({
                        "kind":       "trade",
                        "token_id":   token_id,
                        "price":      price,
                        "size":       size,
                        "side":       side,
                        "maker_addr": maker_addr,
                        "taker_addr": taker_addr,
                    })

# ==================== SIZING HELPERS ====================

def _price_based_size(price: float) -> float:
    """
    Tier bankroll percentage by market price for price_based wallets.
      price < 0.30  → 0.6% of compounding_bankroll
      0.30–0.70     → 1.0%
      price > 0.70  → 3.0%
    """
    if price < 0.30:
        pct = 0.006
    elif price <= 0.70:
        pct = 0.010
    else:
        pct = 0.030
    return cfg.compounding_bankroll * pct


def _calc_size(config: dict, price: float, source_value: float = 0.0) -> float:
    """
    Return position size in USD based on wallet risk_type.
      fixed       -> fixed_risk % of bankroll (e.g. Coniyr)
      price_based -> tiered % of bankroll by market price (TheSpirit, Viser, Kruto)

    Special case - Kruto only (copy_sub_dollar: True):
      If the tiered size is < $1.00, fall back to 1:1 mirror of the source
      wallet's actual position value instead of rejecting the trade.
    """
    if config.get("risk_type") == "fixed":
        return cfg.compounding_bankroll * config.get("fixed_risk", 0.025)

    tiered = _price_based_size(price)

    if tiered < 1.0 and config.get("copy_sub_dollar", False) and source_value > 0:
        return source_value

    return tiered


# ==================== COPY TRADER ====================
class CopyTrader:
    """
    Two-layer signal architecture
    --------------------------------
    Layer 1 — WebSocket  (~0–5 s)   : _on_ws_signal()   — fires on confirmed trade/fill events.
                                       Catches market orders and fast GTC fills immediately.
    Layer 2 — REST poll  (~30–60 s) : scan_and_copy()   — authoritative fallback.
                                       Catches slow-filling GTC limits (seen as open positions)
                                       and handles fill confirmation, exit detection, and price
                                       updates.  Also covers WS reconnect gaps.

    Dedup is handled by a single shared pos_key in SeenTradesStore.
    Whichever layer fires first marks the key seen; the other skips silently.
    Every PendingLimitBuy and Position carries signal_source ("ws"|"rest")
    for observability in logs and the dashboard.
    """

    def __init__(self, dry_run: bool = True):
        self.dry_run          = dry_run
        self.balance          = RobustBalanceManager()
        self.positions:       Dict[str, Position]        = {}
        self.pending:         Dict[str, PendingLimitBuy] = {}
        self.closed_positions: list                      = []
        self.executor         = PolymarketExecutor(dry_run)
        self.seen             = SeenTradesStore(SEEN_TRADES_FILE, DATABASE_URL)

        self._first_scan_done: Set[str] = set()

        # WebSocket state
        self._ws_price_queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
        self._ws_tracked:     Set[str]      = set()
        self._ws_listener:    Optional[PolymarketWSListener] = None

        if WEBSOCKETS_AVAILABLE:
            self._ws_listener = PolymarketWSListener(
                token_ids          = self._ws_tracked,
                ws_price_queue     = self._ws_price_queue,
                on_trade_callback  = self._on_ws_signal,   # immediate execution path
            )
            logging.info("PolymarketWSListener initialised with trade callback")
        else:
            logging.warning("WebSocket listener inactive — install websockets to enable")

        logging.info(f"CopyTrader V2 started | mode={'DRY RUN' if dry_run else 'LIVE'}")

    # ------------------------------------------------------------------
    # WS Layer 1 — immediate signal handler
    # ------------------------------------------------------------------

    async def _on_ws_signal(self, ev: dict):
        """
        Called directly by PolymarketWSListener on every confirmed fill event.
        Runs immediately — no poll-loop wait.

        Flow:
          1. Check if either maker or taker is a tracked wallet.
          2. Dedup against seen + pending (same key as REST path — zero double-execution).
          3. Place limit buy immediately.
          4. Mark seen and register pending so REST poll skips it on next cycle.
          5. Live-subscribe token to WS for price tracking.

        Note: WS events don't carry conditionId/question — those are backfilled
        by the REST poll when it reconciles pending positions.
        """
        # Guard: drawdown / pause check
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until:
            return
        is_broken, _ = self.balance.check_drawdown()
        if is_broken:
            return

        tracked_wallets = {addr.lower(): addr for addr in cfg.WALLETS}
        maker, taker    = ev.get("maker_addr", ""), ev.get("taker_addr", "")

        # Identify which tracked wallet is involved (maker or taker)
        matched_lower = next(
            (w for w in tracked_wallets if w in (maker, taker)), None
        )
        if not matched_lower:
            return

        # Resolve original-case key for cfg.WALLETS lookup
        matched_addr = tracked_wallets[matched_lower]
        config       = cfg.WALLETS.get(matched_addr) or cfg.WALLETS.get(matched_lower)
        if not config:
            return

        token_id = ev["token_id"]
        side     = ev["side"]
        pos_key  = f"{matched_lower}_{token_id}_{side}"

        # Dedup — if REST already handled this, skip
        if self.seen.is_seen(pos_key) or pos_key in self.pending:
            return

        if len(self.positions) >= MAX_POSITIONS:
            logging.warning(f"[WS] Position limit reached — skipping {config['name']} signal.")
            return

        # Pricing first — needed for price_based sizing
        best_ask, mid_price = self.get_orderbook_prices(token_id)
        if best_ask <= 0:
            actual_price = mid_price
        else:
            premium      = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
            actual_price = min(best_ask, mid_price * (1.0 + premium))

        if actual_price <= 0 or actual_price >= 1.0:
            logging.error(f"[WS] Invalid price {actual_price} for {token_id[:12]} — aborting.")
            return

        # Size (price_based tiers by actual_price; fixed uses fixed_risk %)
        # For Kruto sub-dollar fallback, pass the fill size from the WS event
        source_value = float(ev.get("size", 0.0)) * actual_price
        my_size = _calc_size(config, actual_price, source_value)

        if my_size < 1.0 and not config.get("copy_sub_dollar", False):
            logging.info(f"[WS] Sub-dollar size (${my_size:.2f}) rejected for {config['name']}.")
            return

        logging.info(
            f"⚡ [WS INSTANT] {config['name']} | {side} "
            f"token {token_id[:12]}… @ {actual_price:.4f} "
            f"(${my_size:.2f}) [signal_source=ws]"
        )

        ok, order_id, _ = self.executor.place_limit_buy(token_id, my_size, actual_price)
        if not ok:
            logging.warning(f"[WS] Order placement failed for {config['name']} — REST poll will retry.")
            # Do NOT mark seen — REST poll fallback will catch it on next cycle
            return

        # Mark seen before REST poll runs so it skips this pos_key
        self.seen.mark_seen(pos_key)

        # Register pending — market_id/question backfilled by REST reconciliation
        self.pending[pos_key] = PendingLimitBuy(
            pos_key       = pos_key,
            token_id      = token_id,
            market_id     = "pending-ws",   # REST poll will reconcile
            question      = f"WS signal — {token_id[:16]}…",
            outcome       = side,
            source_wallet = matched_addr,
            source_name   = config["name"],
            limit_price   = actual_price,
            size_usd      = my_size,
            order_id      = order_id,
            signal_source = "ws",
        )

        # Live-subscribe token for price updates
        if self._ws_listener and token_id not in self._ws_tracked:
            asyncio.create_task(self._ws_listener.subscribe_token(token_id))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_positions_sync(self, wallet_addr: str) -> Optional[list]:
        url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50"
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(url, timeout=12)
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 404:
                    return []
                else:
                    logging.warning(f"[REST] HTTP {resp.status_code} for {wallet_addr[:10]}")
            except Exception as e:
                logging.warning(f"[REST] Attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return None

    async def _fetch_all_wallets(self) -> Dict[str, Optional[list]]:
        loop         = asyncio.get_event_loop()
        wallet_addrs = list(cfg.WALLETS.keys())
        tasks        = [
            loop.run_in_executor(None, self._get_positions_sync, addr)
            for addr in wallet_addrs
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        out = {}
        for addr, result in zip(wallet_addrs, results):
            if isinstance(result, Exception):
                logging.warning(f"[REST] Exception for {addr[:10]}: {result}")
                out[addr] = None
            else:
                out[addr] = result
        return out

    def get_orderbook_prices(self, token_id: str) -> Tuple[float, float]:
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
                    mid      = (
                        (best_bid + best_ask) / 2
                        if best_bid and best_ask
                        else (best_bid or best_ask or 0.50)
                    )
                    return best_ask, mid
            except Exception as e:
                logging.warning(f"Orderbook request error: {e}")
                time.sleep(1)
        return 0.0, 0.50

    def get_market_question(self, market_id: str) -> str:
        if not market_id or market_id in ("unknown", "pending-ws"):
            return "Polymarket Asset"
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(
                    f"https://clob.polymarket.com/markets/{market_id}", timeout=8
                )
                if r.status_code == 200:
                    return r.json().get("question", "Polymarket Asset")
            except Exception:
                time.sleep(1)
        return "Polymarket Asset"

    def _reconcile_ws_pending(self, raw_by_wallet: Dict[str, Optional[list]]):
        """
        Backfill market_id and question on pending positions that were opened by
        the WS path (which doesn't carry conditionId).  Called each REST poll cycle.
        """
        for pos_key, pending in self.pending.items():
            if pending.market_id != "pending-ws":
                continue
            wallet_raw = raw_by_wallet.get(pending.source_wallet) or []
            for rest_pos in wallet_raw:
                if rest_pos.get("asset") == pending.token_id:
                    market_id = rest_pos.get("conditionId", "unknown")
                    question  = self.get_market_question(market_id)
                    pending.market_id = market_id
                    pending.question  = question
                    logging.info(
                        f"[WS→REST] Reconciled pending '{question[:40]}' "
                        f"for {pending.source_name}"
                    )
                    break

    def clean_expired_limit_orders(self):
        now = datetime.now()
        for k, p in list(self.pending.items()):
            if (now - p.placed_at).total_seconds() >= LIMIT_EXPIRY_SECONDS:
                logging.info(
                    f"[EXPIRY] Limit order expired for {p.source_name} "
                    f"[signal_source={p.signal_source}] — cancelling…"
                )
                if self.executor.cancel_order(p.order_id):
                    del self.pending[k]

    def process_pending_fills(self):
        for k, p in list(self.pending.items()):
            if self.executor.is_order_filled(p.order_id):
                logging.info(
                    f"✨ [FILL] {p.source_name} | {p.outcome} | "
                    f"signal_source={p.signal_source}"
                )
                self.positions[k] = Position(
                    market_id     = p.market_id,
                    question      = p.question,
                    outcome       = p.outcome,
                    token_id      = p.token_id,
                    entry_price   = p.limit_price,
                    size_usd      = p.size_usd,
                    shares        = round(p.size_usd / p.limit_price, 4),
                    source_wallet = p.source_wallet,
                    source_name   = p.source_name,
                    order_id      = p.order_id,
                    current_price = p.limit_price,
                    signal_source = p.signal_source,
                )
                del self.pending[k]

    # ------------------------------------------------------------------
    # Layer 1b — drain WS price queue (keeps dashboard P&L live)
    # ------------------------------------------------------------------

    async def _drain_ws_price_queue(self):
        """Flush price_update events queued by the WS listener — no order logic here."""
        drained = 0
        while not self._ws_price_queue.empty():
            try:
                ev = self._ws_price_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            token_id = ev.get("token_id", "")
            price    = ev.get("price", 0.0)
            if token_id and price:
                for pos in self.positions.values():
                    if pos.token_id == token_id:
                        pos.current_price = price
            drained += 1
        if drained:
            logging.debug(f"[WS] Drained {drained} price update(s)")

    # ------------------------------------------------------------------
    # Layer 3 — REST poll (authoritative fallback + reconciliation)
    # ------------------------------------------------------------------

    async def scan_and_copy(self):
        """
        Poll loop body.  Runs every SCAN_INTERVAL seconds.

        Responsibilities:
          - Drain WS price queue (dashboard freshness)
          - Fetch all wallet positions from REST API simultaneously
          - Reconcile WS-pending positions (backfill market_id/question)
          - REST fallback: place orders for any position WS missed (e.g. slow GTC fills,
            reconnect gaps)
          - Confirm pending fills, handle exits, update prices from REST
        """
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until:
            return

        is_broken, dd_pct = self.balance.check_drawdown()
        if is_broken:
            logging.critical(f"🛑 DRAWDOWN TRIGGERED ({dd_pct*100:.1f}%) — pausing 48 h.")
            cfg.bot_paused_until = datetime.now() + timedelta(hours=48)
            return

        current_bal = self.balance.get_balance()
        if current_bal is None:
            return

        self.clean_expired_limit_orders()
        self.process_pending_fills()

        # Layer 1b: flush WS price updates
        await self._drain_ws_price_queue()

        logging.info(
            f"Poll | Balance: ${current_bal:.2f} | "
            f"Positions: {len(self.positions)} | "
            f"Pending: {len(self.pending)} | "
            f"WS tokens: {len(self._ws_tracked)}"
        )

        # Layer 3: REST fetch — authoritative source
        all_wallet_data = await self._fetch_all_wallets()

        # Backfill WS-pending positions with market metadata
        self._reconcile_ws_pending(all_wallet_data)

        for wallet_addr, config in cfg.WALLETS.items():
            raw = all_wallet_data.get(wallet_addr)
            if raw is None:
                logging.warning(f"[REST] Failed to fetch positions for {config['name']}.")
                continue

            source_token_ids = {
                pos.get("asset") for pos in raw
                if pos.get("asset") and float(pos.get("size", pos.get("shares", 0))) > 0
            }

            logging.info(
                f"[REST] {config['name']} — {len(raw)} position(s), "
                f"{len(source_token_ids)} active tokens"
            )

            # First-scan snapshot (new_only mode)
            if wallet_addr not in self._first_scan_done:
                if config.get("copy_mode") == "new_only":
                    pre_existing = [
                        f"{wallet_addr.lower()}_{pos.get('asset')}_{pos.get('side','YES').upper()}"
                        for pos in raw
                        if pos.get("asset") and float(pos.get("size", pos.get("shares", 0))) > 0
                    ]
                    self.seen.snapshot_existing(pre_existing)
                self._first_scan_done.add(wallet_addr)

            for pos in raw:
                token_id  = pos.get("asset")
                shares    = float(pos.get("size", pos.get("shares", 0)))
                side      = pos.get("side", "YES").upper()
                market_id = pos.get("conditionId", "unknown")

                if not token_id or shares <= 0:
                    continue

                pos_key = f"{wallet_addr.lower()}_{token_id}_{side}"

                # Skip if WS already handled this position
                if self.seen.is_seen(pos_key) or pos_key in self.pending:
                    continue

                # ---- REST FALLBACK: WS missed this trade (slow GTC / reconnect gap) ----
                if len(self.positions) >= MAX_POSITIONS:
                    logging.warning(f"[REST] Position limit reached — skipping REST fallback.")
                    continue

                best_ask, mid_price = self.get_orderbook_prices(token_id)
                if best_ask <= 0:
                    actual_price = mid_price
                else:
                    premium      = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
                    actual_price = min(best_ask, mid_price * (1.0 + premium))

                if actual_price <= 0 or actual_price >= 1.0:
                    logging.error(f"[REST] Invalid price {actual_price} — skipping.")
                    continue

                # Size (price_based tiers by actual_price; fixed uses fixed_risk %)
                # For Kruto sub-dollar fallback, pass the source wallet's position value
                source_value = float(pos.get("initialValue", pos.get("value", 0.0)))
                my_size = _calc_size(config, actual_price, source_value)

                if my_size < 1.0 and not config.get("copy_sub_dollar", False):
                    logging.info(f"[REST] Sub-dollar size (${my_size:.2f}) rejected.")
                    continue

                question_str = self.get_market_question(market_id)
                logging.info(
                    f"🔁 [REST FALLBACK] {config['name']} | {side} | "
                    f"'{question_str[:40]}' @ {actual_price:.4f} "
                    f"[signal_source=rest]"
                )

                ok, order_id, _ = self.executor.place_limit_buy(token_id, my_size, actual_price)
                if ok:
                    self.seen.mark_seen(pos_key)

                    if self._ws_listener and token_id not in self._ws_tracked:
                        asyncio.create_task(self._ws_listener.subscribe_token(token_id))

                    self.pending[pos_key] = PendingLimitBuy(
                        pos_key       = pos_key,
                        token_id      = token_id,
                        market_id     = market_id,
                        question      = question_str,
                        outcome       = side,
                        source_wallet = wallet_addr,
                        source_name   = config["name"],
                        limit_price   = actual_price,
                        size_usd      = my_size,
                        order_id      = order_id,
                        signal_source = "rest",
                    )

            # Update current prices from REST (only if WS hasn't set a fresher value)
            cur_price_map = {
                pos.get("asset"): float(pos.get("curPrice", 0))
                for pos in raw
                if pos.get("asset") and float(pos.get("curPrice", 0)) > 0
            }
            for _pos in self.positions.values():
                if _pos.source_wallet == wallet_addr and _pos.token_id in cur_price_map:
                    rest_price = cur_price_map[_pos.token_id]
                    if rest_price > 0:
                        _pos.current_price = rest_price

            # Exit detection — source wallet closed position
            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr:
                    continue
                if position.token_id not in source_token_ids and position.status == "open":
                    logging.info(
                        f"📉 [EXIT] {position.source_name} closed position — "
                        f"syncing sell [signal_source={position.signal_source}]"
                    )
                    exit_price, _ = self.get_orderbook_prices(position.token_id)
                    ok, _         = self.executor.place_sell(position.token_id, position.shares)
                    if ok:
                        pnl                 = (exit_price - position.entry_price) * position.shares
                        position.status     = "closed"
                        position.exit_price = exit_price
                        position.pnl        = pnl
                        if pnl > 0:
                            cfg.compounding_bankroll += pnl * cfg.COMPOUNDING_RATE
                        self.closed_positions.append(position)
                        del self.positions[pos_key]

# ==================== WEB DASHBOARD ====================
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
        .badge-live   {{ background: #064e3b; color: #6ee7b7; border: 1px solid #065f46; }}
        .badge-dry    {{ background: #1e1b4b; color: #a5b4fc; border: 1px solid #312e81; }}
        .badge-paused {{ background: #450a0a; color: #fca5a5; border: 1px solid #7f1d1d; }}
        .badge-ws     {{ background: #083344; color: #67e8f9; border: 1px solid #155e75; }}
        .badge-src-ws       {{ background: #083344; color: #67e8f9; font-size: 0.62rem; padding: 1px 6px; border-radius: 999px; }}
        .badge-src-rest     {{ background: #1e1b4b; color: #a5b4fc; font-size: 0.62rem; padding: 1px 6px; border-radius: 999px; }}
        .timestamp    {{ font-size: 0.75rem; color: #64748b; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin-bottom: 24px; }}
        .stat-card {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px; padding: 18px 20px; }}
        .stat-label {{ font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.6px; color: #64748b; margin-bottom: 6px; }}
        .stat-value {{ font-size: 1.6rem; font-weight: 700; color: #f1f5f9; line-height: 1; }}
        .stat-sub {{ font-size: 0.75rem; color: #475569; margin-top: 5px; }}
        .pos {{ color: #34d399; }} .neg {{ color: #f87171; }} .neu {{ color: #94a3b8; }}
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
        .market-name {{ font-weight: 500; color: #e2e8f0; max-width: 280px; }}
        .outcome-pill {{ display: inline-block; font-size: 0.68rem; font-weight: 700; padding: 2px 8px; border-radius: 999px; text-transform: uppercase; }}
        .outcome-yes {{ background: #064e3b; color: #6ee7b7; }}
        .outcome-no  {{ background: #450a0a; color: #fca5a5; }}
        .source-tag  {{ font-size: 0.70rem; font-weight: 600; color: #818cf8; background: #1e1b4b; padding: 2px 8px; border-radius: 999px; }}
        .price-mono  {{ font-family: 'Courier New', monospace; font-size: 0.80rem; }}
        .pnl-cell    {{ font-weight: 700; font-size: 0.83rem; white-space: nowrap; }}
        .empty {{ padding: 32px 20px; text-align: center; color: #334155; font-size: 0.85rem; }}
        .empty-icon  {{ font-size: 1.8rem; margin-bottom: 8px; }}
    </style>
</head>
<body>
<div class="page">
    <div class="header">
        <div>
            <div class="header-title">🤖 Poly<span>CopyTrader</span></div>
            <div class="timestamp">Updated {last_updated} &nbsp;·&nbsp; Auto-refresh 15s</div>
        </div>
        <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
            <span class="badge {mode_badge}">{mode_label}</span>
            <span class="badge {status_badge}">{status_label}</span>
            <span class="badge badge-ws">⚡ WS {ws_token_count} tokens</span>
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
        <div class="section-header">
            <span class="section-title">Open Positions</span>
            <span class="count-pill">{open_count}</span>
        </div>
        {positions_block}
    </div>
    <div class="section">
        <div class="section-header">
            <span class="section-title">Closed Trades</span>
            <span class="count-pill">{closed_count}</span>
        </div>
        {closed_block}
    </div>
</div>
</body>
</html>
"""

def _signal_badge(source: str) -> str:
    cls = {
        "ws":   "badge-src-ws",
        "rest": "badge-src-rest",
    }.get(source, "badge-src-rest")
    return f'<span class="{cls}">{source}</span>'

def build_dashboard(bot) -> dict:
    def _sign(v): return "+" if v > 0 else ("-" if v < 0 else "")
    def _cls(v):  return "pos" if v > 0 else ("neg" if v < 0 else "neu")

    bankroll  = bot.balance.cached_balance or 0.0
    drawdown  = ((cfg.peak_bankroll - bankroll) / cfg.peak_bankroll * 100) if cfg.peak_bankroll > 0 else 0.0
    is_paused = bool(cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until)

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

        pos_rows += f"""
        <tr>
            <td><span class="source-tag">{p.source_name}</span>&nbsp;{_signal_badge(p.signal_source)}</td>
            <td class="market-name">{p.question[:55]}</td>
            <td><span class="outcome-pill {outcome_cls}">{p.outcome}</span></td>
            <td>${p.size_usd:.2f}<br><span style="font-size:0.70rem;color:#475569;">{p.shares:.4f} shares</span></td>
            <td class="price-mono">{p.entry_price:.3f}</td>
            <td class="price-mono">{cur_str}</td>
            <td class="pnl-cell {pnl_cls}">{pnl_str}</td>
        </tr>"""

    positions_block = (
        f'<div class="tbl-wrap"><table>'
        f'<thead><tr><th>Source</th><th>Market</th><th>Side</th><th>Size</th>'
        f'<th>Entry</th><th>Current</th><th>Unreal PnL</th></tr></thead>'
        f'<tbody>{pos_rows}</tbody></table></div>'
        if pos_rows else
        '<div class="empty"><div class="empty-icon">📭</div>No open positions</div>'
    )

    closed_list = getattr(bot, "closed_positions", [])
    realised    = sum(p.pnl for p in closed_list)
    closed_rows = ""
    for p in reversed(closed_list):
        outcome_cls = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        pnl_str     = f"{_sign(p.pnl)}${abs(p.pnl):.2f}"
        closed_rows += f"""
        <tr>
            <td><span class="source-tag">{p.source_name}</span>&nbsp;{_signal_badge(p.signal_source)}</td>
            <td class="market-name">{p.question[:55]}</td>
            <td><span class="outcome-pill {outcome_cls}">{p.outcome}</span></td>
            <td class="price-mono">{p.entry_price:.3f}</td>
            <td class="price-mono">{p.exit_price:.3f}</td>
            <td class="pnl-cell {_cls(p.pnl)}">{pnl_str}</td>
        </tr>"""

    closed_block = (
        f'<div class="tbl-wrap"><table>'
        f'<thead><tr><th>Source</th><th>Market</th><th>Side</th>'
        f'<th>Entry</th><th>Exit</th><th>Realised PnL</th></tr></thead>'
        f'<tbody>{closed_rows}</tbody></table></div>'
        if closed_rows else
        '<div class="empty"><div class="empty-icon">📋</div>No closed trades yet</div>'
    )

    total_pnl  = realised + unrealised
    def _fmt(v): return f"{abs(v):.4f}" if abs(v) < 0.005 else f"{abs(v):.2f}"
    comp_delta = cfg.compounding_bankroll - (bot.balance.peak_balance or INITIAL_BANKROLL)

    return {
        "last_updated":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode_label":     mode_label,
        "mode_badge":     mode_badge,
        "status_label":   status_label,
        "status_badge":   status_badge,
        "ws_token_count": len(bot._ws_tracked),
        "balance":        bankroll,
        "peak":           cfg.peak_bankroll,
        "drawdown":       drawdown,
        "dd_cls":         "neg" if drawdown > 10 else ("neu" if drawdown > 5 else "pos"),
        "max_dd":         MAX_DRAWDOWN * 100,
        "comp_bankroll":  cfg.compounding_bankroll,
        "comp_cls":       _cls(comp_delta),
        "comp_rate":      cfg.COMPOUNDING_RATE * 100,
        "total_pnl_cls":  _cls(total_pnl),
        "total_pnl_sign": _sign(total_pnl),
        "total_pnl_abs":  _fmt(total_pnl),
        "unreal_cls":     _cls(unrealised),
        "unreal_sign":    _sign(unrealised),
        "unreal_abs":     _fmt(unrealised),
        "real_cls":       _cls(realised),
        "real_sign":      _sign(realised),
        "real_abs":       _fmt(realised),
        "open_count":     len(bot.positions),
        "closed_count":   len(closed_list),
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

    def log_message(self, format, *args):
        pass

def run_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    logging.info(f"🌐 Dashboard live at http://0.0.0.0:{HEALTH_PORT}")
    server.serve_forever()
