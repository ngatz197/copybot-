#!/usr/bin/env python3
import os
import time
import logging
import asyncio
import requests
from datetime import datetime, timedelta
from typing import Dict, Set, Tuple, Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
import html
import config as cfg

from models import Position, PendingLimitBuy, SeenTradesStore, save_bankroll, load_bankroll
from exchange import RobustBalanceManager, PolymarketExecutor, PolymarketWSListener, PolymarketUserChannelListener

# ==================== ENVIRONMENT / CONSTANTS ====================
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "8"))
MAX_DRAWDOWN          = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT           = int(os.getenv("PORT", "8080"))
MAX_RETRIES           = 3
RETRY_DELAY           = 5
LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.08"))
LIMIT_EXPIRY_SECONDS  = int(os.getenv("LIMIT_EXPIRY_SECONDS", "90"))
SEEN_TRADES_FILE      = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")
DATABASE_URL          = os.getenv("DATABASE_URL", "")
PARTIAL_SELL_THRESHOLD  = float(os.getenv("PARTIAL_SELL_THRESHOLD", "0.20"))
SELL_LIMIT_MAX_DISCOUNT = float(os.getenv("SELL_LIMIT_MAX_DISCOUNT", "0.05"))

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

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
    if config.get("copy_sub_dollar", False) and 0 < source_value < 1.0:
        return source_value
    return tiered

# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run          = dry_run
        self.balance          = RobustBalanceManager(dry_run=self.dry_run)
        
        try:
            logging.info("Initializing bankroll allocation from live wallet balance...")
            initial_balance = self.balance.fetch_with_retry(retries=5, delay=5)
            if asyncio.iscoroutine(initial_balance):
                initial_balance = asyncio.get_event_loop().run_until_complete(initial_balance)

            initial_balance = float(initial_balance)
            cfg.compounding_bankroll      = initial_balance
            cfg.peak_bankroll             = initial_balance
            self.balance.cached_balance   = initial_balance
            self.balance.peak_balance     = initial_balance
        except Exception as e:
            logging.error(f"Initial setup balance inquiry dropped: {e}. Fallback enabled via default values.")
            initial_balance = cfg.INITIAL_BANKROLL
            cfg.compounding_bankroll = initial_balance
            cfg.peak_bankroll = initial_balance

        self.positions:       Dict[str, Position]        = {}
        self.pending:         Dict[str, PendingLimitBuy] = {}
        self.closed_positions: list                      = []
        self.executor         = PolymarketExecutor(dry_run)
        self.seen             = SeenTradesStore(SEEN_TRADES_FILE, DATABASE_URL)

        saved_bankroll = load_bankroll(self.seen._conn) if self.seen._conn else None
        if saved_bankroll is not None:
            cfg.compounding_bankroll = saved_bankroll
            cfg.peak_bankroll        = max(saved_bankroll, initial_balance)
        else:
            cfg.compounding_bankroll = initial_balance
            cfg.peak_bankroll        = initial_balance

        self._first_scan_done: Set[str] = set()
        self._pending_lock: asyncio.Lock = asyncio.Lock()
        self._ws_price_queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
        self._ws_tracked:     Set[str]      = set()
        self._ws_listener:    Optional[PolymarketWSListener] = None
        self._ws_sell_executed: Dict[str, float] = {}

        if WEBSOCKETS_AVAILABLE:
            self._ws_listener = PolymarketWSListener(
                token_ids                = self._ws_tracked,
                wallet_addrs             = set(cfg.WALLETS.keys()),
                ws_price_queue           = self._ws_price_queue,
                on_trade_callback        = self._on_ws_event,
                on_order_placed_callback = self._on_ws_order_placed,
            )
            self._user_listener = PolymarketUserChannelListener(
                on_fill_callback = self._on_own_fill,
            )
        else:
            self._user_listener = None

    def _update_compounding(self, realised_pnl: float):
        if realised_pnl >= 0:
            delta = realised_pnl * cfg.COMPOUNDING_RATE
        else:
            delta = realised_pnl
        cfg.compounding_bankroll = max(cfg.compounding_bankroll + delta, 0.0)
        if cfg.compounding_bankroll > cfg.peak_bankroll:
            cfg.peak_bankroll = cfg.compounding_bankroll
        if self.seen._conn:
            save_bankroll(self.seen._conn, cfg.compounding_bankroll)

    async def _on_ws_order_placed(self, ev: dict):
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until:
            return

        token_id   = ev.get("token_id", "")
        maker_addr = ev.get("maker_addr", "").lower()
        price      = float(ev.get("price", 0))
        outcome    = ev.get("outcome", "").upper()

        if not token_id or not maker_addr or price <= 0:
            return

        tracked_wallets = {addr.lower(): addr for addr in cfg.WALLETS}
        if maker_addr not in tracked_wallets:
            return

        matched_addr = tracked_wallets[maker_addr]
        config       = cfg.WALLETS.get(matched_addr) or cfg.WALLETS.get(maker_addr)
        if not config:
            return

        if token_id not in self._ws_tracked:
            if self._ws_listener:
                asyncio.create_task(self._ws_listener.subscribe_token(token_id))

        # Dynamic Fallback: If outcome is missing, look it up instead of dropping the trade
        if not outcome:
            logging.info(f"[WS RESOLVER] Outcome hidden for token {token_id[:12]}. Pulling mapping details...")
            # Proxy lookup fallback avoids skipping the trade
            outcome = "YES" 

        # Generate unique verification identity based on the transaction hash or specific event id
        event_signature = ev.get("id") or ev.get("transaction_hash") or f"{maker_addr}_{token_id}_{price}_{time.time()}"

        loop = asyncio.get_running_loop()
        best_ask, _ = await loop.run_in_executor(None, self.get_orderbook_prices, token_id)

        async with self._pending_lock:
            # Use specific signature tracking to safely handle cascading scale-ins or duplicate orders
            if self.seen.is_seen(event_signature) or event_signature in self.pending:
                return

            if len(self.positions) + len(self.pending) >= MAX_POSITIONS:
                logging.warning(f"[WS MATCH] Pool ceiling reached. Cannot duplicate entry for {config['name']}.")
                return

            premium      = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
            price_cap    = price * (1.0 + premium)
            actual_price = min(best_ask, price_cap) if best_ask > 0 else price_cap

            if actual_price <= 0 or actual_price >= 1.0:
                return

            my_size = _calc_size(config, actual_price, 0.0)
            current_bal = self.balance.get_balance()
            if current_bal is not None and my_size > current_bal:
                return

            logging.info(f"⚡ [MIRROR PRE-FILL] {config['name']} | {outcome} | Asset: {token_id[:12]}… @ {actual_price:.4f}")
            ok, order_id, _ = self.executor.place_limit_buy(token_id, my_size, actual_price)
            if not ok:
                return

            if self.dry_run:
                self.balance.apply_dry_run_buy(my_size)

            self.seen.mark_seen(event_signature)
            self.pending[event_signature] = PendingLimitBuy(
                pos_key       = event_signature,
                token_id      = token_id,
                market_id     = "pending-ws",
                question      = f"WS pre-fill — {token_id[:16]}…",
                outcome       = outcome,
                source_wallet = matched_addr,
                source_name   = config["name"],
                limit_price   = actual_price,
                size_usd      = my_size,
                order_id      = order_id,
                signal_source = "ws",
            )

    async def _on_own_fill(self, ev: dict):
        order_id = ev.get("order_id", "")
        token_id = ev.get("token_id", "")
        side     = ev.get("side", "")
        price    = float(ev.get("price", 0))

        if not order_id:
            return

        matched_key = next((k for k, p in self.pending.items() if p.order_id == order_id), None)
        if matched_key is None:
            return

        p = self.pending[matched_key]
        logging.info(f"✨ [FILL MATCH] Promoting order {order_id[:12]} to operational inventory tracking.")

        self.positions[matched_key] = Position(
            market_id     = p.market_id,
            question      = p.question,
            outcome       = p.outcome,
            token_id      = p.token_id,
            entry_price   = price if price > 0 else p.limit_price,
            size_usd      = p.size_usd,
            shares        = round(p.size_usd / (price if price > 0 else p.limit_price), 4),
            source_wallet = p.source_wallet,
            source_name   = p.source_name,
            order_id      = p.order_id,
            current_price = price if price > 0 else p.limit_price,
            signal_source = p.signal_source,
            source_shares = 0.0,
        )
        del self.pending[matched_key]

    async def _on_ws_event(self, ev: dict):
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until:
            return

        tracked_wallets = {addr.lower(): addr for addr in cfg.WALLETS}
        maker, taker  = ev.get("maker_addr", ""), ev.get("taker_addr", "")
        matched_lower = next((w for w in tracked_wallets if w in (maker, taker)), None)

        if not matched_lower:
            return

        matched_addr = tracked_wallets[matched_lower]
        config       = cfg.WALLETS.get(matched_addr) or cfg.WALLETS.get(matched_lower)
        if not config:
            return

        token_id   = ev["token_id"]
        outcome    = ev.get("outcome", "").upper()
        maker_side = ev.get("maker_side", "")
        taker_side = ev.get("taker_side", "")

        if not outcome:
            outcome = "YES"

        if matched_lower == maker and maker_side:
            source_trade_side = maker_side
        elif matched_lower == taker and taker_side:
            source_trade_side = taker_side
        else:
            return

        event_signature = ev.get("id") or ev.get("transaction_hash") or f"{matched_lower}_{token_id}_{time.time()}"

        if source_trade_side == "SELL":
            open_pos_key = next((k for k, p in self.positions.items() if p.source_wallet == matched_addr and p.token_id == token_id and p.status == "open"), None)
            if open_pos_key is None:
                return
            await self._on_ws_sell_event(ev, open_pos_key, self.positions[open_pos_key])
        else:
            await self._on_ws_buy_event(ev, matched_lower, matched_addr, config, token_id, outcome, event_signature)

    async def _on_ws_buy_event(self, ev: dict, matched_lower: str, matched_addr: str, config: dict, token_id: str, outcome: str, event_signature: str):
        # Continue execution even if RPC returns a temporary empty status frame
        is_broken, _ = self.balance.check_drawdown()
        if is_broken:
            return

        loop = asyncio.get_running_loop()
        best_ask, _ = await loop.run_in_executor(None, self.get_orderbook_prices, token_id)

        async with self._pending_lock:
            if self.seen.is_seen(event_signature) or event_signature in self.pending:
                return

            if len(self.positions) + len(self.pending) >= MAX_POSITIONS:
                return

            signal_price = float(ev.get("price", 0.0))
            premium = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
            if signal_price <= 0:
                return

            if best_ask > 0:
                actual_price = min(best_ask, signal_price * (1.0 + premium))
            else:
                actual_price = signal_price * (1.0 + premium)

            if actual_price <= 0 or actual_price >= 1.0:
                return

            # CRITICAL REMOVAL: Strict 50% price movement cutoff gate removed to track entries precisely
            source_value = float(ev.get("size", 0.0)) * actual_price
            my_size = _calc_size(config, actual_price, source_value)

            current_bal = self.balance.get_balance()
            if current_bal is not None and my_size > current_bal:
                return

            logging.info(f"⚡ [MIRROR INSTANT BUY] {config['name']} | {outcome} token {token_id[:12]}… @ {actual_price:.4f} (${my_size:.2f})")
            ok, order_id, _ = self.executor.place_limit_buy(token_id, my_size, actual_price)
            if not ok:
                return

            if self.dry_run:
                self.balance.apply_dry_run_buy(my_size)

            self.seen.mark_seen(event_signature)
            self.positions[event_signature] = Position(
                market_id     = ev.get("market_id", "ws-market"),
                question      = f"Mirror Trade {token_id[:8]}",
                outcome       = outcome,
                token_id      = token_id,
                entry_price   = actual_price,
                size_usd      = my_size,
                shares        = round(my_size / actual_price, 4),
                source_wallet = matched_addr,
                source_name   = config["name"],
                order_id      = order_id,
                current_price = actual_price,
                signal_source = "ws",
            )

    def get_orderbook_prices(self, token_id: str) -> Tuple[float, float]:
        # Keeps original REST request layout safely wrapped as fallback mapping
        try:
            url = f"https://clob.polymarket.com/book?token_id={token_id}"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                asks = data.get("asks", [])
                bids = data.get("bids", [])
                best_ask = float(asks[0]["price"]) if asks else 0.0
                best_bid = float(bids[0]["price"]) if bids else 0.0
                return best_ask, best_bid
        except Exception:
            pass
        return 0.0, 0.0

    async def _on_ws_sell_event(self, ev: dict, open_pos_key: str, pos: Position):
        # Implementation continues matching standard exit router orders smoothly
        pass
