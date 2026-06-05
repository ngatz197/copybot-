#!/usr/bin/env python3
import os
import time
import json
import logging
import asyncio
import requests
from typing import Tuple, Optional, Set, Callable, Awaitable
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

MAX_DRAWDOWN          = float(os.getenv("MAX_DRAWDOWN", "0.20"))
MAX_RETRIES           = 3
RETRY_DELAY           = 5
PUSD_CONTRACT_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

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

    def check_drawdown(self) -> Tuple[Optional[bool], float]:
        current = self.get_balance()
        if current is None or self.peak_balance == 0:
            return None, 0.0
        dd = (self.peak_balance - current) / self.peak_balance
        return dd >= MAX_DRAWDOWN, dd

    def apply_dry_run_buy(self, amount_usd: float):
        if self.dry_run and self.cached_balance is not None:
            self.cached_balance -= amount_usd
            logging.info(f"[DRY RUN] Deducted virtual funds: ${amount_usd:.2f} | Balance: ${self.cached_balance:.2f}")

    def apply_dry_run_sell(self, return_usd: float, realised_pnl: float):
        if self.dry_run and self.cached_balance is not None:
            self.cached_balance += return_usd
            if realised_pnl >= 0:
                delta = realised_pnl * cfg.COMPOUNDING_RATE
            else:
                delta = realised_pnl
            cfg.compounding_bankroll = max(cfg.compounding_bankroll + delta, 0.0)
            if cfg.compounding_bankroll > cfg.peak_bankroll:
                cfg.peak_bankroll = cfg.compounding_bankroll
            if self.cached_balance > self.peak_balance:
                self.peak_balance = self.cached_balance
                cfg.peak_bankroll = max(cfg.peak_bankroll, self.cached_balance)
            logging.info(
                f"[DRY RUN] Sell return=${return_usd:.2f} | "
                f"pnl={realised_pnl:+.4f} | delta={delta:+.4f} | "
                f"sizing_base=${cfg.compounding_bankroll:.2f} | "
                f"balance=${self.cached_balance:.2f}"
            )

    def apply_dry_run_cancel(self, amount_usd: float):
        if self.dry_run and self.cached_balance is not None:
            self.cached_balance += amount_usd
            logging.info(
                f"[DRY RUN] Cancel refund=${amount_usd:.2f} | "
                f"balance=${self.cached_balance:.2f} | "
                f"sizing_base=${cfg.compounding_bankroll:.2f} (unchanged)"
            )

# ==================== EXECUTOR (V2) ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client  = None
        self._dry_run_fill_counter: dict = {}
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
                result = self.client.create_and_post_order(
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
            if order_id not in self._dry_run_fill_counter:
                self._dry_run_fill_counter[order_id] = 0
            self._dry_run_fill_counter[order_id] += 1
            return self._dry_run_fill_counter[order_id] >= 2
        try:
            order = self.client.get_order(order_id)
            status = order.get("status", "").upper()
            return status in ("FILLED", "CLOSED")
        except Exception as e:
            logging.debug(f"Error checking order status: {e}")
            return False

# ==================== LIVE WEBSOCKET LISTENER ====================
class PolymarketWSListener:
    def __init__(self, token_ids: Set[str], wallet_addrs: Set[str], ws_price_queue: asyncio.Queue,
                 on_trade_callback: Callable[[dict], Awaitable[None]],
                 on_order_placed_callback: Callable[[dict], Awaitable[None]]):
        self.token_ids = token_ids
        self.wallet_addrs = {w.lower() for w in wallet_addrs}
        self.ws_price_queue = ws_price_queue
        self.on_trade = on_trade_callback
        self.on_order_placed = on_order_placed_callback
        self._ws = None

    async def run(self):
        if not WEBSOCKETS_AVAILABLE:
            logging.error("Websockets module missing — cannot initialize listener stream.")
            return
        
        uri = "wss://clob.polymarket.com/ws/market"
        while True:
            try:
                logging.info(f"Connecting to Polymarket Public Market Feed: {uri}")
                async with websockets.connect(uri) as ws:
                    self._ws = ws
                    # Seed subscriptions with any already discovered tokens
                    if self.token_ids:
                        await self._send_subscription(list(self.token_ids))
                    
                    async for raw_message in ws:
                        # Wrap message ingestion directly to eliminate silent crash events
                        try:
                            await self._handle_message(raw_message)
                        except Exception as inner_ex:
                            logging.error(f"[WS PARSE EXCEPTION] Failed parsing event frame: {inner_ex} | Raw: {raw_message}", exc_info=True)
            except Exception as e:
                logging.error(f"WebSocket interface dropped connection: {e}. Retrying in 5 seconds...", exc_info=True)
                await asyncio.sleep(5)

    async def subscribe_token(self, token_id: str):
        if token_id in self.token_ids:
            return
        self.token_ids.add(token_id)
        if self._ws and self._ws.open:
            try:
                await self._send_subscription([token_id])
                logging.info(f"[WS SUB] Registered tracking for asset token: {token_id}")
            except Exception as e:
                logging.warning(f"Failed to submit live WS subscription frame for asset {token_id}: {e}")

    async def _send_subscription(self, token_ids: list):
        payload = {
            "type": "subscribe",
            "assets_ids": token_ids,
            "channels": ["trades", "book"]
        }
        await self._ws.send(json.dumps(payload))

    async def _handle_message(self, raw: str):
        data = json.loads(raw)
        if not isinstance(data, dict):
            return

        ev_type = data.get("event_type", "").upper()
        if ev_type == "BOOK":
            # Direct book updates to pricing queues
            token_id = data.get("asset_id")
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            if token_id and (bids or asks):
                best_bid = float(bids[0]["price"]) if bids else 0.0
                best_ask = float(asks[0]["price"]) if asks else 0.0
                if best_bid > 0 or best_ask > 0:
                    try:
                        self.ws_price_queue.put_nowait((token_id, best_bid, best_ask))
                    except asyncio.QueueFull:
                        pass
            return

        # Intercept and process trade execution updates immediately
        if ev_type in ("TRADE", "ORDER_PLACED"):
            maker = data.get("maker_addr", "").lower()
            taker = data.get("taker_addr", "").lower()
            
            # Pure Mirror rule: If any entity matches a targeted wallet profile, dispatch instantly
            if maker in self.wallet_addrs or taker in self.wallet_addrs:
                if ev_type == "TRADE":
                    await self.on_trade(data)
                elif ev_type == "ORDER_PLACED":
                    await self.on_order_placed(data)

# ==================== LIVE USER CHANNEL FEED LISTENER ====================
class PolymarketUserChannelListener:
    def __init__(self, on_fill_callback: Callable[[dict], Awaitable[None]]):
        self.on_fill_callback = on_fill_callback
        self._ws = None

    async def run(self):
        if not WEBSOCKETS_AVAILABLE or not YOUR_PRIVATE_KEY:
            logging.info("Private key absent or websockets unavailable — disabling own-fills loop.")
            return
        
        uri = "wss://clob.polymarket.com/ws/user"
        while True:
            try:
                logging.info("Connecting to private endpoint authentication layer...")
                async with websockets.connect(uri) as ws:
                    self._ws = ws
                    # Authenticate user connection workspace session
                    timestamp = str(int(time.time()))
                    nonce = 0
                    # Standard API login registration sequence would deploy here
                    
                    async for raw in ws:
                        await self._handle_message(raw)
            except Exception as e:
                logging.debug(f"[USER-WS] Disconnected: {e}. Reconnecting in 10s...")
                await asyncio.sleep(10)

    async def _handle_message(self, raw: str):
        try:
            events = json.loads(raw)
        except json.JSONDecodeError:
            return

        if not isinstance(events, list):
            events = [events]

        for ev in events:
            ev_type = (ev.get("event_type") or ev.get("type") or "").lower()
            if ev_type not in ("order_fill", "order_filled", "trade"):
                continue

            token_id = ev.get("asset_id") or ev.get("market") or ""
            price    = float(ev.get("price", 0))
            size     = float(ev.get("size", 0))
            side     = (ev.get("side") or "").upper()
            order_id = ev.get("id") or ev.get("order_id") or ""

            if not token_id or not price:
                continue

            logging.info(f"[USER-WS] Live Confirmation Match: {side} {size:.4f} shares @ {price:.4f}")
            if self.on_fill_callback:
                await self.on_fill_callback({
                    "kind":     "own_fill",
                    "token_id": token_id,
                    "price":    price,
                    "side":     side,
                    "order_id": order_id
                })
