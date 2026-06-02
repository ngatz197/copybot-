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

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
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
        # During dry runs, ignore blockchain sync intervals to safeguard mutating virtual balance states
        if self.dry_run and self.cached_balance is not None:
            return self.cached_balance

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

    def apply_dry_run_buy(self, amount_usd: float):
        if self.dry_run and self.cached_balance is not None:
            self.cached_balance -= amount_usd
            cfg.compounding_bankroll = self.cached_balance
            logging.info(f"[DRY RUN] Deducted virtual funds: ${amount_usd:.2f} | Balance: ${self.cached_balance:.2f}")

    def apply_dry_run_sell(self, return_usd: float):
        if self.dry_run and self.cached_balance is not None:
            self.cached_balance += return_usd
            cfg.compounding_bankroll = self.cached_balance
            if self.cached_balance > self.peak_balance:
                self.peak_balance = self.cached_balance
                cfg.peak_bankroll = self.cached_balance
            logging.info(f"[DRY RUN] Credited virtual return: ${return_usd:.2f} | Balance: ${self.cached_balance:.2f}")


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
        self.token_ids          = token_ids          
        self.ws_price_queue     = ws_price_queue
        self.on_trade_callback  = on_trade_callback  
        self._running           = False
        self._ws                = None
        self._subscribed: Set[str] = set()

    async def subscribe_token(self, token_id: str):
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
        try:
            events = json.loads(raw)
        except json.JSONDecodeError:
            return

        if not isinstance(events, list):
            events = [events]

        for ev in events:
            ev_type = ev.get("event_type") or ev.get("type") or ""

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
                        try:
                            self.ws_price_queue.get_nowait()
                            self.ws_price_queue.put_nowait({
                                "kind": "price_update", "token_id": token_id, "price": price,
                            })
                        except Exception:
                            pass

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
    if price < 0.30:
        pct = 0.006
    elif price <= 0.70:
        pct = 0.010
    else:
        pct = 0.030
    return cfg.compounding_bankroll * pct


def _calc_size(config: dict, price: float, source_value: float = 0.0) -> float:
    if config.get("risk_type") == "fixed":
        return cfg.compounding_bankroll * config.get("fixed_risk", 0.025)

    tiered = _price_based_size(price)

    if tiered < 1.0 and config.get("copy_sub_dollar", False) and source_value > 0:
        return source_value

    return tiered


# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run          = dry_run
        self.balance          = RobustBalanceManager(dry_run=self.dry_run)
        
        # Seed compounding references dynamically across both execution pathways
        try:
            logging.info("Initializing bankroll allocation from live wallet balance...")
            initial_balance = self.balance.fetch_with_retry(retries=5, delay=5)
            cfg.compounding_bankroll = initial_balance
            cfg.peak_bankroll        = initial_balance
        except Exception as e:
            logging.error(f"Critical initialization failure: {e}")
            raise SystemExit("Exiting bot: Unable to ascertain initial balance configuration.")

        self.positions:       Dict[str, Position]        = {}
        self.pending:         Dict[str, PendingLimitBuy] = {}
        self.closed_positions: list                      = []
        self.executor         = PolymarketExecutor(dry_run)
        self.seen             = SeenTradesStore(SEEN_TRADES_FILE, DATABASE_URL)

        self._first_scan_done: Set[str] = set()

        self._ws_price_queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
        self._ws_tracked:     Set[str]      = set()
        self._ws_listener:    Optional[PolymarketWSListener] = None

        if WEBSOCKETS_AVAILABLE:
            self._ws_listener = PolymarketWSListener(
                token_ids          = self._ws_tracked,
                ws_price_queue     = self._ws_price_queue,
                on_trade_callback  = self._on_ws_signal,   
            )
            logging.info("PolymarketWSListener initialised with trade callback")
        else:
            logging.warning("WebSocket listener inactive — install websockets to enable")

        logging.info(f"CopyTrader V2 started | mode={'DRY RUN' if dry_run else 'LIVE'}")

    async def _on_ws_signal(self, ev: dict):
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until:
            return
        is_broken, _ = self.balance.check_drawdown()
        if is_broken:
            return

        tracked_wallets = {addr.lower(): addr for addr in cfg.WALLETS}
        maker, taker    = ev.get("maker_addr", ""), ev.get("taker_addr", "")

        matched_lower = next(
            (w for w in tracked_wallets if w in (maker, taker)), None
        )
        if not matched_lower:
            return

        matched_addr = tracked_wallets[matched_lower]
        config       = cfg.WALLETS.get(matched_addr) or cfg.WALLETS.get(matched_lower)
        if not config:
            return

        token_id = ev["token_id"]
        side     = ev["side"]
        pos_key  = f"{matched_lower}_{token_id}_{side}"

        if self.seen.is_seen(pos_key) or pos_key in self.pending:
            return

        if len(self.positions) >= MAX_POSITIONS:
            logging.warning(f"[WS] Position limit reached — skipping {config['name']} signal.")
            return

        best_ask, mid_price = self.get_orderbook_prices(token_id)
        if best_ask <= 0:
            actual_price = mid_price
        else:
            premium      = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
            actual_price = min(best_ask, mid_price * (1.0 + premium))

        if actual_price <= 0 or actual_price >= 1.0:
            logging.error(f"[WS] Invalid price {actual_price} for {token_id[:12]} — aborting.")
            return

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
            return

        # Handle local dry-run compounding balance adjustments immediately 
        if self.dry_run:
            self.balance.apply_dry_run_buy(my_size)

        self.seen.mark_seen(pos_key)

        self.pending[pos_key] = PendingLimitBuy(
            pos_key       = pos_key,
            token_id      = token_id,
            market_id     = "pending-ws",   
            question      = f"WS signal — {token_id[:16]}…",
            outcome       = side,
            source_wallet = matched_addr,
            source_name   = config["name"],
            limit_price   = actual_price,
            size_usd      = my_size,
            order_id      = order_id,
            signal_source = "ws",
        )

        if self._ws_listener and token_id not in self._ws_tracked:
            asyncio.create_task(self._ws_listener.subscribe_token(token_id))

    def handle_position_exit(self, token_id: str, shares: float, actual_exit_price: float):
        """
        Call this function inside your position scanning logic when an exit is confirmed.
        """
        ok, order_id = self.executor.place_sell(token_id, shares)
        if ok and self.dry_run:
            estimated_return = shares * actual_exit_price
            self.balance.apply_dry_run_sell(estimated_return)

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
                    mid      = (best_bid + best_ask) / 2.0 if (best_bid and best_ask) else best_bid or best_ask or 0.5
                    return best_ask, mid
            except Exception as e:
                logging.warning(f"Error fetching orderbook for {token_id[:12]}: {e}")
                time.sleep(1)
        return 0.0, 0.0


# ==================== HEALTH SERVER ====================
class HealthRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/health", "/"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {"status": "healthy", "timestamp": datetime.now().isoformat()}
            self.wfile.write(json.dumps(response).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress standard HTTP logging to keep your clean terminal feed clear
        return


def run_health_server():
    try:
        server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthRequestHandler)
        logging.info(f"🚀 Health server running on port {HEALTH_PORT}")
        server.serve_forever()
    except Exception as e:
        logging.error(f"Failed to start health server: {e}")
