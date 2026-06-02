#!/usr/bin/env python3
import os
import json
import time
import asyncio
import logging
import threading
from datetime import datetime
from typing import Dict, Set, Tuple, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
import config as cfg

# ==================== OPTIONAL DEPENDENCIES ====================
try:
    from py_clob_client_v2 import ClobClient, OrderArgs, MarketOrderArgs, OrderType, Side, ApiCreds, PartialCreateOrderOptions
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
    logging.warning("websockets not installed — WS listener disabled.")

# ==================== ENVIRONMENT / CONSTANTS ====================
YOUR_PRIVATE_KEY      = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET           = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
POLY_API_KEY          = os.getenv("POLY_API_KEY", "")
POLY_SECRET           = os.getenv("POLY_SECRET", "")
POLY_PASSPHRASE       = os.getenv("POLY_PASSPHRASE", "")
DATABASE_URL          = os.getenv("DATABASE_URL", "")

MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "8"))
MAX_DRAWDOWN          = float(os.getenv("MAX_DRAWDOWN", "0.20"))
MAX_RETRIES           = 3
RETRY_DELAY           = 5
LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))
SEEN_TRADES_FILE      = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")
PUSD_CONTRACT_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

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
    signal_source: str   = "rest"

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
    signal_source: str      = "rest"
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
        except Exception as e:
            logging.error(f"Postgres init failed: {e} — falling back to local file")
            self._conn = None
            self._load_file()

    def _load_postgres(self) -> Set[str]:
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT pos_key FROM seen_trades")
                return {row[0] for row in cur.fetchall()}
        except Exception:
            return set()

    def _save_postgres(self, pos_key: str):
        try:
            with self._conn.cursor() as cur:
                cur.execute("INSERT INTO seen_trades (pos_key) VALUES (%s) ON CONFLICT DO NOTHING", (pos_key,))
        except Exception:
            self._reconnect_postgres()

    def _save_postgres_many(self, keys):
        if not keys: return
        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, "INSERT INTO seen_trades (pos_key) VALUES %s ON CONFLICT DO NOTHING", [(k,) for k in keys])
        except Exception:
            self._reconnect_postgres()

    def _reconnect_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require")
            self._conn.autocommit = True
        except Exception:
            pass

    def _load_file(self):
        try:
            with open(self.filepath, "r") as f:
                data = json.load(f)
                self._seen = set(data) if isinstance(data, list) else set()
        except Exception:
            self._seen = set()
        self.backend = "local-file"

    def _save_file(self):
        try:
            with open(self.filepath, "w") as f:
                json.dump(sorted(self._seen), f)
        except Exception:
            pass

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

# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    POLYGON_RPCS = [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com",
        "https://polygon.drpc.org",
    ]

    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.cached_balance: Optional[float] = None
        self.virtual_balance: Optional[float] = None
        self.last_update = 0
        self.peak_balance = 0.0

    def _fetch_balance(self) -> float:
        if not YOUR_WALLET:
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
            except Exception:
                pass
        return 0.0

    def get_balance(self, force=False) -> Optional[float]:
        if self.dry_run and self.virtual_balance is not None:
            cfg.compounding_bankroll = self.virtual_balance
            return self.virtual_balance

        if force or self.cached_balance is None or (time.time() - self.last_update > 30):
            real = self._fetch_balance()
            if real > 0:
                self.cached_balance = real
                self.last_update    = time.time()
                
                cfg.compounding_bankroll = real
                
                if self.dry_run and self.virtual_balance is None:
                    self.virtual_balance = real
                    logging.info(f"Initialized dry-run virtual balance from network: ${real:.2f}")

                if real > self.peak_balance:
                    self.peak_balance  = real
                    cfg.peak_bankroll  = real
            else:
                if self.cached_balance is None:
                    logging.error("Could not fetch real pUSD balance.")
        
        return self.cached_balance

    def fetch_with_retry(self, retries: int = 5, delay: int = 10) -> float:
        for attempt in range(1, retries + 1):
            val = self._fetch_balance()
            if val > 0:
                self.cached_balance = val
                self.peak_balance   = val
                self.last_update    = time.time()
                cfg.compounding_bankroll = val
                if self.dry_run:
                    self.virtual_balance = val
                logging.info(f"Real pUSD balance confirmed: ${val:.2f}")
                return val
            time.sleep(delay)
        raise RuntimeError("Could not fetch real pUSD balance.")

    def check_drawdown(self) -> Tuple[bool, float]:
        current = self.get_balance()
        if current is None or self.peak_balance == 0:
            return False, 0.0
        dd = (self.peak_balance - current) / self.peak_balance
        return dd >= MAX_DRAWDOWN, dd

# ==================== EXECUTOR (V2) ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool, balance_manager: RobustBalanceManager):
        self.dry_run = dry_run
        self.bm = balance_manager
        self.client  = None
        if not dry_run and CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(api_key=POLY_API_KEY, api_secret=POLY_SECRET, api_passphrase=POLY_PASSPHRASE)
                self.client = ClobClient(host="https://clob.polymarket.com", chain_id=137, key=YOUR_PRIVATE_KEY, creds=creds)
            except Exception as e:
                logging.error(f"ClobClient V2 init failed: {e}")

    def place_limit_buy(self, token_id: str, amount_usd: float, limit_price: float) -> Tuple[bool, str, float]:
        shares = round(amount_usd / limit_price, 4)
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] LIMIT BUY {shares:.4f} shares @ {limit_price:.4f} (${amount_usd:.2f})")
            if self.bm.virtual_balance is not None:
                self.bm.virtual_balance -= amount_usd
                cfg.compounding_bankroll = self.bm.virtual_balance
            return True, "dry-run-limit-buy", limit_price
            
        for attempt in range(MAX_RETRIES):
            try:
                result   = self.client.create_and_post_order(
                    order_args = OrderArgs(token_id=token_id, price=limit_price, size=shares, side=Side.BUY),
                    options    = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.GTC,
                )
                order_id = result.get("orderID", result.get("id", "unknown"))
                return True, order_id, limit_price
            except Exception:
                time.sleep(RETRY_DELAY)
        return False, "", limit_price

    def cancel_order(self, order_id: str) -> bool:
        if self.dry_run or self.client is None:
            return True
        try:
            self.client.cancel(order_id)
            return True
        except Exception:
            return False

    def is_order_filled(self, order_id: str) -> bool:
        if self.dry_run or self.client is None:
            return True
        try:
            status = self.client.get_order(order_id).get("status", "").lower()
            return status in ("matched", "filled")
        except Exception:
            return False

    def place_sell(self, token_id: str, shares: float, estimated_price: float = 0.50) -> Tuple[bool, str]:
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] MARKET SELL {shares:.4f} shares")
            if self.bm.virtual_balance is not None:
                revenue = shares * estimated_price
                self.bm.virtual_balance += revenue
                cfg.compounding_bankroll = self.bm.virtual_balance
            return True, "dry-run-sell"
            
        for attempt in range(MAX_RETRIES):
            try:
                result   = self.client.create_and_post_market_order(
                    order_args = MarketOrderArgs(token_id=token_id, amount=shares, side=Side.SELL),
                    options    = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.FOK,
                )
                order_id = result.get("orderID", result.get("id", "unknown"))
                return True, order_id
            except Exception:
                time.sleep(RETRY_DELAY)
        return False, ""

# ==================== WEBSOCKET LISTENER ====================
class PolymarketWSListener:
    WS_URL         = "wss://ws-subscriptions-clob.polymarket.com/ws/"
    PING_INTERVAL  = 20
    RECONNECT_BASE =  2
    RECONNECT_MAX  = 60

    def __init__(self, token_ids: Set[str], ws_price_queue: asyncio.Queue, on_trade_callback: Optional[Callable[[dict], Awaitable[None]]] = None):
        self.token_ids          = token_ids
        self.ws_price_queue     = ws_price_queue
        self.on_trade_callback  = on_trade_callback
        self._running           = False
        self._ws                = None
        self._subscribed: Set[str] = set()

    async def subscribe_token(self, token_id: str):
        if token_id in self._subscribed: return
        self.token_ids.add(token_id)
        if self._ws is not None:
            try:
                await self._send_subscribe(self._ws, {token_id})
                self._subscribed.add(token_id)
            except Exception:
                pass

    async def run(self):
        if not WEBSOCKETS_AVAILABLE: return
        self._running = True
        delay = self.RECONNECT_BASE
        while self._running:
            try:
                await self._connect_and_listen()
                delay = self.RECONNECT_BASE
            except Exception:
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.RECONNECT_MAX)

    def stop(self):
        self._running = False

    async def _connect_and_listen(self):
        async with websockets.connect(self.WS_URL, ping_interval=self.PING_INTERVAL, ping_timeout=30, close_timeout=10) as ws:
            self._ws = ws
            self._subscribed.clear()
            if self.token_ids:
                await self._send_subscribe(ws, self.token_ids)
                self._subscribed.update(self.token_ids)

            async for raw in ws:
                if not self._running: break
                await self._handle_message(raw)
        self._ws = None

    async def _send_subscribe(self, ws, token_ids: Set[str]):
        for channel in ("market", "trade"):
            payload = {"type": "subscribe", "channel": channel, "asset_ids": list(token_ids)}
            await ws.send(json.dumps(payload))

    async def _handle_message(self, raw: str):
        try:
            events = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(events, list): events = [events]

        for ev in events:
            ev_type = ev.get("event_type") or ev.get("type") or ""
            if ev_type in ("price_change", "book", "last_trade_price"):
                token_id = ev.get("asset_id") or ev.get("market") or ""
                price    = float(ev.get("price", 0)) or float(ev.get("mid_price", 0)) or float(ev.get("last_trade_price", 0))
                if token_id and price:
                    try:
                        self.ws_price_queue.put_nowait({"kind": "price_update", "token_id": token_id, "price": price})
                    except asyncio.QueueFull:
                        try:
                            self.ws_price_queue.get_nowait()
                            self.ws_price_queue.put_nowait({"kind": "price_update", "token_id": token_id, "price": price})
                        except Exception: pass
            elif ev_type in ("trade", "order_filled"):
                token_id   = ev.get("asset_id") or ev.get("market") or ""
                price      = float(ev.get("price", 0))
                size       = float(ev.get("size", 0))
                side       = (ev.get("side") or ev.get("outcome") or "YES").upper()
                maker_addr = (ev.get("maker_address") or ev.get("maker") or "").lower()
                taker_addr = (ev.get("taker_address") or ev.get("taker") or "").lower()
                if token_id and price and self.on_trade_callback:
                    await self.on_trade_callback({"kind": "trade", "token_id": token_id, "price": price, "size": size, "side": side, "maker_addr": maker_addr, "taker_addr": taker_addr})

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
        self.balance          = RobustBalanceManager(dry_run)
        self.positions:       Dict[str, Position]        = {}
        self.pending:         Dict[str, PendingLimitBuy] = {}
        self.closed_positions: list                      = []
        self.executor         = PolymarketExecutor(dry_run, self.balance)
        self.seen             = SeenTradesStore(SEEN_TRADES_FILE, DATABASE_URL)
        self._first_scan_done: Set[str] = set()

        self._ws_price_queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
        self._ws_tracked:     Set[str]      = set()
        self._ws_listener:    Optional[PolymarketWSListener] = None

        if WEBSOCKETS_AVAILABLE:
            self._ws_listener = PolymarketWSListener(token_ids=self._ws_tracked, ws_price_queue=self._ws_price_queue, on_trade_callback=self._on_ws_signal)
        
        try:
            self.balance.get_balance(force=True)
        except Exception as e:
            logging.error(f"Failed initial baseline sync: {e}")

    async def _on_ws_signal(self, ev: dict):
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until: return
        is_broken, _ = self.balance.check_drawdown()
        if is_broken: return

        tracked_wallets = {addr.lower(): addr for addr in cfg.WALLETS}
        maker, taker    = ev.get("maker_addr", ""), ev.get("taker_addr", "")
        matched_lower = next((w for w in tracked_wallets if w in (maker, taker)), None)
        if not matched_lower: return

        matched_addr = tracked_wallets[matched_lower]
        config       = cfg.WALLETS.get(matched_addr) or cfg.WALLETS.get(matched_lower)
        if not config: return

        token_id = ev["token_id"]
        side     = ev["side"]
        pos_key  = f"{matched_lower}_{token_id}_{side}"

        if self.seen.is_seen(pos_key) or pos_key in self.pending: return
        if len(self.positions) >= MAX_POSITIONS: return

        best_ask, mid_price = self.get_orderbook_prices(token_id)
        actual_price = min(best_ask, mid_price * (1.0 + config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM))) if best_ask > 0 else mid_price

        if actual_price <= 0 or actual_price >= 1.0: return

        source_value = float(ev.get("size", 0.0)) * actual_price
        my_size = _calc_size(config, actual_price, source_value)

        if my_size < 1.0 and not config.get("copy_sub_dollar", False): return

        ok, order_id, _ = self.executor.place_limit_buy(token_id, my_size, actual_price)
        if not ok: return

        self.seen.mark_seen(pos_key)
        self.pending[pos_key] = PendingLimitBuy(
            pos_key=pos_key, token_id=token_id, market_id="pending-ws", question=f"WS signal — {token_id[:16]}…",
            outcome=side, source_wallet=matched_addr, source_name=config["name"], limit_price=actual_price,
            size_usd=my_size, order_id=order_id, signal_source="ws"
        )

        if self._ws_listener and token_id not in self._ws_tracked:
            asyncio.create_task(self._ws_listener.subscribe_token(token_id))

    def _get_positions_sync(self, wallet_addr: str) -> Optional[list]:
        url = f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50"
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(url, timeout=12)
                if resp.status_code == 200: return resp.json()
                if resp.status_code == 404: return []
            except Exception:
                time.sleep(RETRY_DELAY)
        return None

    async def _fetch_all_wallets(self) -> Dict[str, Optional[list]]:
        loop         = asyncio.get_event_loop()
        wallet_addrs = list(cfg.WALLETS.keys())
        tasks        = [loop.run_in_executor(None, self._get_positions_sync, addr) for addr in wallet_addrs]
        results      = await asyncio.gather(*tasks, return_exceptions=True)
        return {addr: (None if isinstance(res, Exception) else res) for addr, res in zip(wallet_addrs, results)}

    def get_orderbook_prices(self, token_id: str) -> Tuple[float, float]:
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8)
                if r.status_code == 200:
                    data = r.json()
                    bids, asks = data.get("bids", []), data.get("asks", [])
                    best_bid = float(bids[0]["price"]) if bids else 0.0
                    best_ask = float(asks[0]["price"]) if asks else 0.0
                    return best_ask, ((best_bid + best_ask) / 2 if best_bid and best_ask else (best_bid or best_ask or 0.50))
            except Exception:
                time.sleep(1)
        return 0.0, 0.50

# ==================== RENDER COMPLIANT HEALTH SERVER ====================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ('/health', '/'):
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"OK")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        return  # Silences background request logging to clean up your logs

def run_health_server():
    port = int(os.getenv("PORT", "8080"))
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    logging.info(f"🚀 Render health check endpoint activated on port {port}")
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
