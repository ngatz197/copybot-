#!/usr/bin/env python3
import os
import time
import json
import hmac
import hashlib
import base64
import logging
import asyncio
import requests
from typing import Tuple, Optional, Set, Callable
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

# ==================== ROBUST BALANCE MANAGER ====================
class RobustBalanceManager:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.cached_balance = cfg.INITIAL_BANKROLL
        self.peak_balance   = cfg.INITIAL_BANKROLL

    def get_balance(self) -> float:
        return self.cached_balance

    async def fetch_with_retry(self, retries: int = 5, delay: int = 5) -> float:
        if self.dry_run:
            return self.cached_balance

        url = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{
                "to": PUSD_CONTRACT_ADDRESS,
                "data": f"0x70a08231000000000000000000000000{YOUR_WALLET[2:] if YOUR_WALLET.startswith('0x') else YOUR_WALLET}"
            }, "latest"],
            "id": 1
        }

        for attempt in range(retries):
            try:
                loop = asyncio.get_running_loop()
                response = await loop.run_in_executor(
                    None, lambda: requests.post(url, json=payload, timeout=10).json()
                )
                hex_bal = response["result"]
                raw_bal = int(hex_bal, 16) / 10**6  # pUSD utilizes 6 decimals
                self.cached_balance = raw_bal
                if raw_bal > self.peak_balance:
                    self.peak_balance = raw_bal
                return raw_bal
            except Exception as e:
                logging.warning(f"Balance fetch attempt {attempt+1} failed: {e}")
                await asyncio.sleep(delay)

        logging.error("All balance query retries exhausted. Resuming loop execution with last cached metrics.")
        return self.cached_balance

    def apply_dry_run_buy(self, amount: float):
        self.cached_balance = max(self.cached_balance - amount, 0.0)

    def apply_dry_run_sell(self, amount: float):
        self.cached_balance += amount
        if self.cached_balance > self.peak_balance:
            self.peak_balance = self.cached_balance

    def check_drawdown(self) -> Tuple[bool, float]:
        if self.peak_balance <= 0:
            return False, 0.0
        current_drawdown = (self.peak_balance - self.cached_balance) / self.peak_balance
        if current_drawdown >= MAX_DRAWDOWN:
            logging.critical(f"🛑 CRITICAL SAFETY TRIP: Maximum protection drawdown exceeded ({current_drawdown*100:.2f}%). Execution pipelines locked.")
            return True, current_drawdown
        return False, current_drawdown


# ==================== POLYMARKET EXECUTOR ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.client = None
        if not dry_run and CLOB_AVAILABLE:
            try:
                creds = ApiCreds(
                    api_key=POLY_API_KEY,
                    secret=POLY_SECRET,
                    passphrase=POLY_PASSPHRASE
                )
                self.client = ClobClient(
                    host="https://clob.polymarket.com",
                    key=YOUR_PRIVATE_KEY,
                    chain_id=137,
                    creds=creds
                )
                logging.info("⚡ Polymarket Live API client established successfully.")
            except Exception as e:
                logging.error(f"Failed to instantiate official CLOB Client: {e}. Coerced to dry_run state safely.")
                self.dry_run = True

    def place_limit_buy(self, token_id: str, size_usd: float, price: float) -> Tuple[bool, str, float]:
        shares = round(size_usd / price, 2)
        if self.dry_run or not self.client:
            sim_id = f"sim_buy_{int(time.time())}"
            logging.info(f"🧪 [SIMULATION] Executed limit buy order target: {shares} shares of token {token_id[:12]}… at price ${price:.4f}")
            return True, sim_id, price

        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=shares,
                side=Side.BUY
            )
            resp = self.client.create_and_post_order(order_args)
            if resp and resp.get("success"):
                return True, resp.get("orderID", f"live_buy_{int(time.time())}"), price
            logging.warning(f"CLOB exchange endpoint rejected buy request metadata: {resp}")
        except Exception as e:
            logging.error(f"Exception encountered during live order transmission: {e}")
        return False, "", 0.0

    def place_limit_sell(self, token_id: str, shares: float, price: float) -> Tuple[bool, str, float]:
        if self.dry_run or not self.client:
            sim_id = f"sim_sell_{int(time.time())}"
            logging.info(f"🧪 [SIMULATION] Executed limit sell order target: {shares} shares of token {token_id[:12]}… at price ${price:.4f}")
            return True, sim_id, price

        try:
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=shares,
                side=Side.SELL
            )
            resp = self.client.create_and_post_order(order_args)
            if resp and resp.get("success"):
                return True, resp.get("orderID", f"live_sell_{int(time.time())}"), price
        except Exception as e:
            logging.error(f"Exception encountered during live order termination: {e}")
        return False, "", 0.0


# ==================== POLYMARKET PUBLIC MARKET WEBSOCKET LISTENER ====================
class PolymarketWSListener:
    def __init__(self, token_ids: Set[str], wallet_addrs: Set[str], ws_price_queue: asyncio.Queue, on_trade_callback: Callable, on_order_placed_callback: Callable):
        self.token_ids = token_ids
        self.wallet_addrs = {w.lower() for w in wallet_addrs}
        self.ws_price_queue = ws_price_queue
        self.on_trade_callback = on_trade_callback
        self.on_order_placed_callback = on_order_placed_callback
        self.uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        self._ws = None

    async def run(self):
        if not WEBSOCKETS_AVAILABLE:
            logging.error("Aborting infrastructure boot sequence — websockets library dependencies are missing.")
            return

        while True:
            try:
                logging.info(f"Connecting to official Polymarket Market Streaming Gateway: {self.uri}")
                async with websockets.connect(self.uri) as ws:
                    self._ws = ws
                    
                    sub_payload = {
                        "type": "subscribe",
                        "channels": ["trades"]
                    }
                    await ws.send(json.dumps(sub_payload))
                    logging.info("WebSocket interface handshakes complete. Base market channels globally subscribed.")

                    for token in list(self.token_ids):
                        await self.subscribe_token(token)

                    async for message in ws:
                        await self._handle_message(message)
            except Exception as err:
                logging.error(f"WebSocket interface dropped connection: {err}. Retrying in 5 seconds...")
                self._ws = None
                await asyncio.sleep(5)

    async def subscribe_token(self, token_id: str):
        if not token_id:
            return
        self.token_ids.add(token_id)
        if self._ws and self._ws.open:
            try:
                payload = {
                    "type": "subscribe",
                    "channels": ["book"],
                    "token_ids": [token_id]
                }
                await self._ws.send(json.dumps(payload))
                logging.debug(f"[MARKET-WS] Dynamically attached monitoring signature context to token: {token_id[:16]}…")
            except Exception as e:
                logging.warning(f"Failed to post dynamic subscription update to open stream: {e}")

    async def _handle_message(self, raw: str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        if isinstance(data, list):
            events = data
        else:
            events = [data]

        for ev in events:
            ev_type = (ev.get("event_type") or ev.get("type") or "").lower()
            
            if ev_type in ("trade", "trades"):
                await self._process_trade_event(ev)
            elif ev_type in ("book", "price_change"):
                token_id = ev.get("token_id") or ev.get("asset_id") or ""
                if token_id:
                    try:
                        await self.ws_price_queue.put(ev)
                    except asyncio.QueueFull:
                        pass

    async def _process_trade_event(self, ev: dict):
        maker = ev.get("maker_addr", "").lower()
        taker = ev.get("taker_addr", "").lower()
        
        if maker in self.wallet_addrs or taker in self.wallet_addrs:
            if self.on_trade_callback:
                await self.on_trade_callback(ev)


# ==================== POLYMARKET USER EXECUTION CHANNEL LISTENER ====================
class PolymarketUserChannelListener:
    def __init__(self, on_fill_callback: Callable):
        self.on_fill_callback = on_fill_callback
        self.uri = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
        self._ws = None

    async def run(self):
        if not WEBSOCKETS_AVAILABLE or not POLY_API_KEY or not POLY_SECRET:
            logging.info("Private operational key configurations not initialized. User Channel listener going dormant.")
            return

        while True:
            try:
                logging.info(f"Connecting to official Polymarket Private Account Tracking Stream: {self.uri}")
                async with websockets.connect(self.uri) as ws:
                    self._ws = ws
                    
                    timestamp = str(int(time.time()))
                    
                    # 1. Compute L2 HMAC Signature Protocol mapping for WebSockets
                    sig_payload = f"{timestamp}GET/ws/user"
                    secret_bytes = base64.b64decode(POLY_SECRET) if isinstance(POLY_SECRET, str) else POLY_SECRET
                    
                    try:
                        signature = hmac.new(
                            secret_bytes, 
                            sig_payload.encode(), 
                            hashlib.sha256
                        ).digest()
                        encoded_sig = base64.b64encode(signature).decode()
                    except Exception as sig_err:
                        logging.error(f"Failed to sign user stream credentials cryptographically: {sig_err}")
                        await asyncio.sleep(10)
                        continue

                    # 2. Strict Object structure framing with nested 'auth' context mapping
                    auth_payload = {
                        "type": "subscribe",
                        "channels": ["user"],
                        "auth": {
                            "apiKey": POLY_API_KEY,
                            "passphrase": POLY_PASSPHRASE,
                            "timestamp": int(timestamp),
                            "signature": encoded_sig
                        }
                    }
                    
                    await ws.send(json.dumps(auth_payload))
                    logging.info("Private workspace authorization context transmitted securely via nested auth matrix structure.")

                    async for message in ws:
                        await self._handle_message(message)
                        
            except Exception as err:
                logging.error(f"[USER-WS] Connection dropped or authentication rejected: {err}. Reconnecting in 10 seconds...")
                self._ws = None
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

            logging.debug(f"[USER-WS] Verified internal fill matched: {side} {size:.4f} @ {price:.4f} asset={token_id[:12]}…")

            if self.on_fill_callback:
                await self.on_fill_callback({
                    "kind":     "own_fill",
                    "token_id": token_id,
                    "price":    price,
                    "size":     size,
                    "side":     side,
                    "order_id": order_id
                })
