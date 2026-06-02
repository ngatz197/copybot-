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
        """
        Returns (is_broken, drawdown_fraction).
        is_broken is None when the balance is unknown — callers must treat
        None as a blocking condition, not as "safe to proceed".
        """
        current = self.get_balance()
        if current is None or self.peak_balance == 0:
            return None, 0.0
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

        # --- Attempt 1: FOK market sell (fast fill) ---
        for attempt in range(MAX_RETRIES):
            try:
                result   = self.client.create_and_post_market_order(
                    order_args = MarketOrderArgs(token_id=token_id, amount=shares, side=Side.SELL),
                    options    = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.FOK,
                )
                order_id = result.get("orderID", result.get("id", "unknown"))
                logging.info(f"MARKET SELL placed (FOK V2): {order_id}")
                return True, order_id
            except Exception as e:
                logging.warning(f"FOK SELL attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)

        # --- Fallback: GTC limit sell at best available bid ---
        logging.critical(
            f"⚠️  FOK sell exhausted for token {token_id[:12]}… — "
            f"falling back to GTC limit sell.  Manual review recommended."
        )
        for attempt in range(MAX_RETRIES):
            try:
                # Place at a penny below mid; adjust tick as needed.
                book     = requests.get(
                    f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8
                ).json()
                bids     = book.get("bids", [])
                limit_px = float(bids[0]["price"]) if bids else 0.01
                result   = self.client.create_and_post_order(
                    order_args = OrderArgs(
                        token_id = token_id,
                        price    = limit_px,
                        size     = shares,
                        side     = Side.SELL,
                    ),
                    options    = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.GTC,
                )
                order_id = result.get("orderID", result.get("id", "unknown"))
                logging.info(f"GTC limit SELL placed as fallback: {order_id} @ {limit_px}")
                return True, order_id
            except Exception as e:
                logging.warning(f"GTC SELL fallback attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)

        logging.critical(
            f"🚨 ALL SELL ATTEMPTS FAILED for token {token_id[:12]}… "
            f"({shares:.4f} shares).  Position is STUCK — manual intervention required."
        )
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