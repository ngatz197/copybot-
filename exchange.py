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
            # Do NOT touch cfg.compounding_bankroll here — it is the sizing base
            # and should only grow via realised profits on sells (mirrors live mode).
            logging.info(f"[DRY RUN] Deducted virtual funds: ${amount_usd:.2f} | Balance: ${self.cached_balance:.2f}")

    def apply_dry_run_sell(self, return_usd: float, realised_pnl: float):
        if self.dry_run and self.cached_balance is not None:
            self.cached_balance += return_usd
            # Mirror live compounding logic exactly:
            #   Wins:   reinvest only COMPOUNDING_RATE fraction of profit
            #   Losses: absorb the full loss immediately (no dampening)
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
            # compounding_bankroll intentionally NOT touched here.
            # A cancelled unfilled order has zero realised PnL — the sizing base
            # must not change. Previously this hard-set compounding_bankroll to
            # cached_balance which wiped accumulated compounding history.
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
            # Simulate a 2-cycle fill delay for more realistic dry-run behaviour.
            count = self._dry_run_fill_counter.get(order_id, 0) + 1
            self._dry_run_fill_counter[order_id] = count
            if count >= 2:
                self._dry_run_fill_counter.pop(order_id, None)
                return True
            return False
        try:
            status = self.client.get_order(order_id).get("status", "").lower()
            return status in ("matched", "filled")
        except Exception as e:
            logging.warning(f"Could not check order status for {order_id}: {e}")
            return False

    def place_sell(self, token_id: str, shares: float, reference_price: float = 0.0) -> Tuple[bool, str]:
        """
        Sell *shares* of token_id.

        Slippage control
        ----------------
        We fetch the live orderbook before placing so we know the best bid.
        We then clamp the limit price to:

            limit_px = max(best_bid, mid * (1 - SELL_LIMIT_MAX_DISCOUNT))

        This means we will never post a sell more than SELL_LIMIT_MAX_DISCOUNT
        below mid-price.  If reference_price > 0 it is used as the mid fallback
        when the orderbook fetch fails (typically the WS signal price).

        Execution sequence
        ------------------
        1. Try a FOK market sell (instant fill at best available bid).
        2. If FOK fails, post a GTC limit sell at the clamped limit price.
        """
        import config as cfg  # imported here to avoid circular at module level

        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] SELL {shares:.4f} shares (ref={reference_price:.4f})")
            return True, "dry-run-sell"

        # ── Fetch live orderbook to derive a slippage-safe limit price ──────
        best_bid = 0.0
        mid      = reference_price  # fallback if orderbook unavailable
        try:
            book     = requests.get(
                f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8
            ).json()
            bids     = book.get("bids", [])
            asks     = book.get("asks", [])
            best_bid = float(bids[0]["price"]) if bids else 0.0
            best_ask = float(asks[0]["price"]) if asks else 0.0
            if best_bid and best_ask:
                mid = (best_bid + best_ask) / 2
            elif best_bid or best_ask:
                mid = best_bid or best_ask
            # else keep reference_price as mid
        except Exception as e:
            logging.warning(f"Orderbook fetch before sell failed: {e} — using reference price")

        if mid <= 0:
            mid = 0.50  # last-resort fallback; should rarely trigger
        floor_px  = round(mid * (1.0 - cfg.SELL_LIMIT_MAX_DISCOUNT), 4)
        limit_px  = max(best_bid, floor_px) if best_bid > 0 else floor_px
        limit_px  = max(limit_px, 0.01)   # never post below 1¢

        logging.info(
            f"[SELL] token={token_id[:12]}… shares={shares:.4f} "
            f"best_bid={best_bid:.4f} mid={mid:.4f} "
            f"floor={floor_px:.4f} limit_px={limit_px:.4f}"
        )

        # --- Attempt 1: FOK market sell (single attempt — fast fill or immediate fallback) ---
        try:
            result   = self.client.create_and_post_market_order(
                order_args = MarketOrderArgs(token_id=token_id, amount=shares, side=Side.SELL),
                options    = PartialCreateOrderOptions(tick_size="0.01"),
                order_type = OrderType.FOK,
            )
            order_id = result.get("orderID", result.get("id", "unknown"))
            logging.info(f"MARKET SELL placed (FOK): {order_id}")
            return True, order_id
        except Exception as e:
            logging.warning(f"FOK SELL failed: {e} — falling through to GTC limit sell")

        # --- Fallback: GTC limit sell at the slippage-clamped price -----------
        logging.warning(
            f"⚠️  FOK sell failed for token {token_id[:12]}… — "
            f"posting GTC limit sell @ {limit_px:.4f}"
        )
        for attempt in range(MAX_RETRIES):
            try:
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
                logging.info(f"GTC limit SELL placed @ {limit_px:.4f}: {order_id}")
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
    WS_URL_MARKET  = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    PING_INTERVAL  = 20
    RECONNECT_BASE =  2
    RECONNECT_MAX  = 60

    def __init__(
        self,
        token_ids:         Set[str],
        ws_price_queue:    asyncio.Queue,
        on_trade_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
    ):
        self.token_ids          = token_ids
        self.ws_price_queue     = ws_price_queue
        self.on_trade_callback  = on_trade_callback
        self._running           = False
        self._ws_market:  Optional[object] = None
        self._subscribed: Set[str] = set()

    async def subscribe_token(self, token_id: str):
        if token_id in self._subscribed:
            return
        self.token_ids.add(token_id)
        if self._ws_market is not None:
            try:
                await self._send_subscribe(self._ws_market, {token_id})
                logging.info(f"[WS] Live-subscribed token {token_id[:12]}…")
                self._subscribed.add(token_id)
            except Exception as e:
                logging.warning(f"[WS] Live subscribe failed for {token_id[:12]}: {e}")

    async def run(self):
        if not WEBSOCKETS_AVAILABLE:
            logging.warning("[WS] websockets not installed — listener inactive.")
            return
        self._running = True
        await self._run_channel()

    async def _run_channel(self):
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
        logging.info(f"[WS] Connecting to {self.WS_URL_MARKET} …")
        async with websockets.connect(
            self.WS_URL_MARKET,
            ping_interval = self.PING_INTERVAL,
            ping_timeout  = 30,
            close_timeout = 10,
        ) as ws:
            self._ws_market = ws
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
        self._ws_market = None

    async def _send_subscribe(self, ws, token_ids: Set[str]):
        payload = {
            "type":      "subscribe",
            "channel":   "market",
            "asset_ids": list(token_ids),
        }
        await ws.send(json.dumps(payload))
        logging.info(f"[WS] Subscribed {len(token_ids)} token(s)")

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
                outcome    = (ev.get("outcome") or "").upper()
                maker_addr = (ev.get("maker_address") or ev.get("maker") or "").lower()
                taker_addr = (ev.get("taker_address") or ev.get("taker") or "").lower()
                # maker_side / taker_side: "BUY" or "SELL" for each leg of the fill.
                # These let the engine resolve the source wallet's actual trade
                # direction without relying on the ambiguous top-level `side` field.
                maker_side = (ev.get("maker_side") or "").upper()
                taker_side = (ev.get("taker_side") or "").upper()

                if token_id and price and self.on_trade_callback:
                    await self.on_trade_callback({
                        "kind":       "trade",
                        "token_id":   token_id,
                        "price":      price,
                        "size":       size,
                        "outcome":    outcome,
                        "maker_addr": maker_addr,
                        "taker_addr": taker_addr,
                        "maker_side": maker_side,
                        "taker_side": taker_side,
                    })

# ==================== USER CHANNEL LISTENER ====================
class PolymarketUserChannelListener:
    """
    Subscribes to the Polymarket user channel for each tracked source wallet.

    The user channel delivers order-level events (placements, fills, cancels)
    scoped to a specific wallet address.  Unlike the market channel, every
    event carries the wallet's own unambiguous trade direction (side=BUY/SELL)
    and market outcome (YES/NO/OVER/UNDER) directly — no counterparty inference
    needed.

    One persistent connection is maintained per tracked wallet.  On reconnect
    all wallets are re-subscribed automatically.

    The subscribe message requires no credentials for public read-only tracking:

        { "type": "subscribe", "channel": "user", "markets": ["<wallet_addr>"] }

    Events are normalised to the same dict schema used by PolymarketWSListener
    so the engine's _on_ws_event callback needs no changes.
    """

    WS_URL_USER    = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    PING_INTERVAL  = 20
    RECONNECT_BASE =  2
    RECONNECT_MAX  = 60

    def __init__(
        self,
        wallet_addrs:      Set[str],                               # source wallets to track
        on_trade_callback: Optional[Callable[[dict], Awaitable[None]]] = None,
    ):
        # wallet_addrs is the live set from cfg.WALLETS — mutations are reflected
        # automatically on the next reconnect.
        self.wallet_addrs      = wallet_addrs
        self.on_trade_callback = on_trade_callback
        self._running          = False
        self._ws:   Optional[object] = None

    async def run(self):
        if not WEBSOCKETS_AVAILABLE:
            logging.warning("[USER-WS] websockets not installed — user channel inactive.")
            return
        self._running = True
        await self._run_channel()

    def stop(self):
        self._running = False

    async def _run_channel(self):
        delay = self.RECONNECT_BASE
        while self._running:
            try:
                await self._connect_and_listen()
                delay = self.RECONNECT_BASE
            except Exception as e:
                logging.warning(f"[USER-WS] Disconnected: {e} — reconnecting in {delay}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.RECONNECT_MAX)

    async def _connect_and_listen(self):
        if not self.wallet_addrs:
            logging.info("[USER-WS] No wallets configured — waiting 30s before retry.")
            await asyncio.sleep(30)
            return

        logging.info(f"[USER-WS] Connecting to {self.WS_URL_USER} …")
        async with websockets.connect(
            self.WS_URL_USER,
            ping_interval = self.PING_INTERVAL,
            ping_timeout  = 30,
            close_timeout = 10,
        ) as ws:
            self._ws = ws
            logging.info("[USER-WS] Connected ✅")

            # Subscribe all tracked wallets in a single message.
            await ws.send(json.dumps({
                "type":    "subscribe",
                "channel": "user",
                "markets": list(self.wallet_addrs),
            }))
            logging.info(f"[USER-WS] Subscribed {len(self.wallet_addrs)} wallet(s)")

            async for raw in ws:
                if not self._running:
                    break
                try:
                    await self._handle_message(raw)
                except Exception as e:
                    logging.debug(f"[USER-WS] Message parse error: {e}")
        self._ws = None

    async def _handle_message(self, raw: str):
        try:
            events = json.loads(raw)
        except json.JSONDecodeError:
            return

        if not isinstance(events, list):
            events = [events]

        for ev in events:
            ev_type = (ev.get("event_type") or ev.get("type") or "").lower()

            # We care about fills — placements and cancels carry no fill price
            # and do not represent a completed position change.
            if ev_type not in ("order_fill", "order_filled", "trade"):
                continue

            token_id = ev.get("asset_id") or ev.get("market") or ""
            price    = float(ev.get("price", 0))
            size     = float(ev.get("size", 0))

            if not token_id or not price:
                continue

            # `side` on the user channel is the wallet's own order direction —
            # BUY or SELL — not a fill leg.  This is the unambiguous signal.
            side    = (ev.get("side") or "").upper()
            outcome = (ev.get("outcome") or "").upper()

            # `owner` / `maker` / `address` — whichever field carries the
            # wallet address that generated this event.  Try every known alias;
            # also check `user` and `trader` which appear in some API versions.
            wallet = (
                ev.get("owner")
                or ev.get("maker_address")
                or ev.get("maker")
                or ev.get("address")
                or ev.get("user")
                or ev.get("trader")
                or ""
            ).lower()

            if not wallet:
                # The connection is scoped to self.wallet_addrs by the subscribe
                # message, so the event must belong to one of those wallets.
                # If there is exactly one subscribed wallet we can resolve
                # unambiguously; otherwise we cannot safely attribute the event
                # and must skip with a warning so the field name is diagnosable.
                if len(self.wallet_addrs) == 1:
                    wallet = next(iter(self.wallet_addrs)).lower()
                    logging.debug(
                        f"[USER-WS] wallet field absent for {token_id[:12]}\u2026 \u2014 "
                        f"resolved from sole subscription address {wallet[:10]}\u2026"
                    )
                else:
                    logging.warning(
                        f"[USER-WS] wallet field absent for {token_id[:12]}\u2026 and "
                        f"{len(self.wallet_addrs)} wallets subscribed \u2014 cannot attribute "
                        f"event; skipping.  Raw keys: {list(ev.keys())}"
                    )
                    continue

            if not side:
                logging.debug(f"[USER-WS] Fill event missing side — skipping {token_id[:12]}…")
                continue

            if not outcome:
                logging.debug(
                    f"[USER-WS] Fill event missing outcome for {token_id[:12]}… — skipping "
                    f"rather than defaulting, to avoid mislabelling position."
                )
                continue

            logging.debug(
                f"[USER-WS] {wallet[:10]}… {side} {outcome} {size} @ {price} "
                f"token={token_id[:12]}…"
            )

            if self.on_trade_callback:
                await self.on_trade_callback({
                    # Use "user_trade" kind so the engine knows direction is
                    # already resolved and can skip the maker/taker inference.
                    "kind":         "user_trade",
                    "token_id":     token_id,
                    "price":        price,
                    "size":         size,
                    "outcome":      outcome,
                    # source_wallet lets _on_ws_event match directly without
                    # scanning maker_addr / taker_addr.
                    "source_wallet": wallet,
                    # trade_side is the wallet's own direction — no ambiguity.
                    "trade_side":   side,
                })
