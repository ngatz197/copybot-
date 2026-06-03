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

from models import Position, PendingLimitBuy, SeenTradesStore
from exchange import RobustBalanceManager, PolymarketExecutor, PolymarketWSListener

# ==================== ENVIRONMENT / CONSTANTS ====================
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "8"))
MAX_DRAWDOWN          = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT           = int(os.getenv("PORT", "8080"))
MAX_RETRIES           = 3
RETRY_DELAY           = 5
LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))
LIMIT_EXPIRY_SECONDS  = int(os.getenv("LIMIT_EXPIRY_SECONDS", "300"))
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

    if tiered < 1.0 and config.get("copy_sub_dollar", False) and 0 < source_value < 1.0:
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
            cfg.compounding_bankroll      = initial_balance
            cfg.peak_bankroll             = initial_balance
            # Seed dry-run virtual balance from the real on-chain balance so
            # deductions and drawdown checks start from the correct baseline.
            self.balance.cached_balance   = initial_balance
            self.balance.peak_balance     = initial_balance
            logging.info(f"Dry-run virtual balance seeded at ${initial_balance:.2f}")
        except Exception as e:
            logging.error(f"Critical initialization failure: {e}")
            raise SystemExit("Exiting bot: Unable to ascertain initial balance configuration.")

        self.positions:       Dict[str, Position]        = {}
        self.pending:         Dict[str, PendingLimitBuy] = {}
        self.closed_positions: list                      = []
        self.executor         = PolymarketExecutor(dry_run)
        self.seen             = SeenTradesStore(SEEN_TRADES_FILE, DATABASE_URL)

        self._first_scan_done: Set[str] = set()

        # Lock that serialises the "check-seen → place-order → mark-seen" critical
        # section so a simultaneous WS signal and REST poll can never both slip
        # through for the same pos_key (fix #14).
        self._pending_lock: asyncio.Lock = asyncio.Lock()

        self._ws_price_queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
        self._ws_tracked:     Set[str]      = set()
        self._ws_listener:    Optional[PolymarketWSListener] = None
        # Tracks (pos_key, sell_fraction_str) pairs that have already been acted
        # on via WS so the REST fallback knows not to re-fire them.
        self._ws_sell_executed: Set[str] = set()

        if WEBSOCKETS_AVAILABLE:
            self._ws_listener = PolymarketWSListener(
                token_ids          = self._ws_tracked,
                ws_price_queue     = self._ws_price_queue,
                on_trade_callback  = self._on_ws_event,
            )
            logging.info("PolymarketWSListener initialised — market channel only")
        else:
            logging.warning("WebSocket listener inactive — install websockets to enable")

        logging.info(f"CopyTrader V2 started | mode={'DRY RUN' if dry_run else 'LIVE'}")

    async def _on_ws_event(self, ev: dict):
        """
        Unified WS trade callback.  The WS layer no longer infers buy vs sell
        from unreliable fields like `trade_side`.  Instead:

        - If we hold an open position for this wallet + token → treat as a sell
          signal and mirror proportionally.
        - If we do NOT hold a position → treat as a buy signal.

        This means a single trade event is always routed correctly regardless of
        which direction field (if any) the exchange happens to include.
        """
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until:
            return

        tracked_wallets = {addr.lower(): addr for addr in cfg.WALLETS}
        maker, taker    = ev.get("maker_addr", ""), ev.get("taker_addr", "")
        matched_lower   = next(
            (w for w in tracked_wallets if w in (maker, taker)), None
        )
        if not matched_lower:
            return

        matched_addr = tracked_wallets[matched_lower]
        config       = cfg.WALLETS.get(matched_addr) or cfg.WALLETS.get(matched_lower)
        if not config:
            return

        copy_mode = config.get("copy_mode", "new_only")
        if copy_mode != "new_only":
            logging.warning(f"[WS BUY] {config['name']} copy_mode='{copy_mode}' not supported — skipping.")
            return

        token_id = ev["token_id"]
        side     = ev["side"].upper()
        pos_key  = f"{matched_lower}_{token_id}_{side}"

        # ── Sell path: we already hold this position ──────────────────────────
        position = self.positions.get(pos_key)
        if position and position.status == "open":
            await self._on_ws_sell_event(ev, pos_key, position)
            return

        # ── Buy path: no open position for this wallet + token ────────────────
        await self._on_ws_buy_event(ev, matched_lower, matched_addr, config, token_id, side, pos_key)

    async def _on_ws_buy_event(
        self,
        ev:           dict,
        matched_lower: str,
        matched_addr:  str,
        config:        dict,
        token_id:      str,
        side:          str,
        pos_key:       str,
    ):
        is_broken, _ = self.balance.check_drawdown()
        if is_broken is None:
            logging.warning("[WS BUY] Balance unknown — skipping signal until balance is confirmed.")
            return
        if is_broken:
            return

        # Fetch orderbook BEFORE acquiring the lock — slow HTTP call.
        loop = asyncio.get_running_loop()
        best_ask, mid_price = await loop.run_in_executor(
            None, self.get_orderbook_prices, token_id
        )

        async with self._pending_lock:
            if self.seen.is_seen(pos_key) or pos_key in self.pending:
                return

            if len(self.positions) + len(self.pending) >= MAX_POSITIONS:
                logging.warning(f"[WS BUY] Position limit reached — skipping {config['name']} signal.")
                return

            signal_price = float(ev.get("price", 0.0))
            if signal_price > 0:
                # Use the actual price the source paid as reference
                if best_ask > 0:
                    premium      = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
                    actual_price = min(best_ask, signal_price * (1.0 + premium))
                else:
                    # Orderbook failed — use source price with small premium
                    premium      = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
                    actual_price = signal_price * (1.0 + premium)
            else:
                if best_ask <= 0:
                    actual_price = mid_price
                else:
                    premium      = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
                    actual_price = min(best_ask, mid_price * (1.0 + premium))

            if actual_price <= 0 or actual_price >= 1.0:
                logging.error(f"[WS BUY] Invalid price {actual_price} for {token_id[:12]} — aborting.")
                return

            # Guard: if we used source price but market is now way higher, skip
            if signal_price > 0 and actual_price > signal_price * 1.50:
                logging.warning(
                    f"[WS BUY] Market moved too far from source price "
                    f"(source={signal_price:.4f}, market={actual_price:.4f}) — skipping {config['name']}."
                )
                return

            source_value = float(ev.get("size", 0.0)) * actual_price
            my_size = _calc_size(config, actual_price, source_value)

            current_bal = self.balance.get_balance()
            if current_bal is not None and my_size > current_bal:
                logging.warning(f"[WS BUY] Order size ${my_size:.2f} exceeds balance ${current_bal:.2f} — skipping {config['name']}.")
                return

            logging.info(
                f"⚡ [WS INSTANT BUY] {config['name']} | {side} "
                f"token {token_id[:12]}… @ {actual_price:.4f} "
                f"(${my_size:.2f}) [signal_source=ws]"
            )

            ok, order_id, _ = self.executor.place_limit_buy(token_id, my_size, actual_price)
            if not ok:
                logging.warning(f"[WS BUY] Order placement failed for {config['name']} — REST poll will retry.")
                return

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

    async def _on_ws_sell_event(self, ev: dict, pos_key: str, position: "Position"):
        """
        Mirror a sell detected via WS.  We use the source wallet's share count
        already stored on the position to compute the sell fraction.
        """
        ws_sold_shares = float(ev.get("size", 0.0))
        if ws_sold_shares <= 0:
            return

        if position.source_shares <= 0:
            logging.info("[WS SELL] source_shares not yet initialized — deferring to REST")
            return

        source_total  = position.source_shares if position.source_shares > 0 else ws_sold_shares
        sell_fraction = min(ws_sold_shares / source_total, 1.0)

        if sell_fraction < PARTIAL_SELL_THRESHOLD:
            # Accumulate sub-threshold reductions; fire when total crosses threshold.
            position.pending_reduction += sell_fraction
            logging.info(
                f"[WS SELL] {position.source_name} sold {sell_fraction:.1%} "
                f"(accumulated={position.pending_reduction:.1%}) — below threshold, accumulating."
            )
            if position.pending_reduction < PARTIAL_SELL_THRESHOLD:
                return
            # Accumulated total now crosses the threshold — fire and reset.
            sell_fraction              = position.pending_reduction
            position.pending_reduction = 0.0

        our_shares_to_sell = round(position.shares * sell_fraction, 4)
        if our_shares_to_sell <= 0:
            return

        dedup_key = f"{pos_key}_{sell_fraction:.2f}"
        if dedup_key in self._ws_sell_executed:
            return
        self._ws_sell_executed.add(dedup_key)
        if len(self._ws_sell_executed) > 2000:
            self._ws_sell_executed.clear()

        signal_price = float(ev.get("price", 0.0))
        logging.info(
            f"⚡ [WS INSTANT SELL] {position.source_name} | {position.outcome} | "
            f"fraction={sell_fraction:.1%} | our_shares={our_shares_to_sell:.4f} | "
            f"ref_price={signal_price:.4f}"
        )

        await self._execute_sell(
            pos_key         = pos_key,
            position        = position,
            shares_to_sell  = our_shares_to_sell,
            reference_price = signal_price,
            trigger         = "[WS SELL]",
        )

    # ------------------------------------------------------------------ #
    #  Sell helpers                                                        #
    # ------------------------------------------------------------------ #

    async def _execute_sell(
        self,
        pos_key:       str,
        position:      "Position",
        shares_to_sell: float,
        reference_price: float,
        trigger:       str,
    ):
        """
        Sell *shares_to_sell* of *position*, update internal state, and
        record PnL.  Works for both full and partial exits.

        *trigger* is a short label for log lines, e.g. "[WS SELL]" or
        "[REST EXIT]".

        Returns True if the sell order was accepted, False otherwise.
        """
        loop = asyncio.get_running_loop()

        # Fetch a fresh orderbook price for PnL accounting (non-blocking).
        exit_ask, exit_mid = await loop.run_in_executor(
            None, self.get_orderbook_prices, position.token_id
        )
        exit_price = exit_mid if exit_mid > 0 else reference_price

        ok, _ = await loop.run_in_executor(
            None,
            lambda: self.executor.place_sell(
                position.token_id, shares_to_sell, reference_price=reference_price
            ),
        )
        if not ok:
            logging.warning(f"{trigger} Sell order failed for {pos_key} — will retry on next poll.")
            return False

        realised_pnl = (exit_price - position.entry_price) * shares_to_sell
        is_full_exit = abs(shares_to_sell - position.shares) < 1e-6

        if is_full_exit:
            position.status     = "closed"
            position.exit_price = exit_price
            position.pnl        = realised_pnl

            if self.dry_run:
                self.balance.apply_dry_run_sell(shares_to_sell * exit_price, realised_pnl)
            else:
                cfg.compounding_bankroll += realised_pnl * cfg.COMPOUNDING_RATE
                cfg.compounding_bankroll  = max(cfg.compounding_bankroll, 0.0)

            self.closed_positions.append(position)
            if len(self.closed_positions) > 500:
                self.closed_positions = self.closed_positions[-500:]
            self.positions.pop(pos_key, None)
            logging.info(
                f"📉 {trigger} FULL EXIT {position.source_name} | "
                f"{position.outcome} | exit={exit_price:.4f} | "
                f"pnl={realised_pnl:+.4f} | signal={position.signal_source}"
            )
        else:
            # Partial sell — shrink our position proportionally.
            position.shares   -= shares_to_sell
            position.size_usd  = position.shares * position.entry_price
            position.pnl      += realised_pnl

            if self.dry_run:
                self.balance.apply_dry_run_sell(shares_to_sell * exit_price, realised_pnl)
            else:
                cfg.compounding_bankroll += realised_pnl * cfg.COMPOUNDING_RATE
                cfg.compounding_bankroll  = max(cfg.compounding_bankroll, 0.0)

            logging.info(
                f"✂️  {trigger} PARTIAL EXIT {position.source_name} | "
                f"{position.outcome} | sold={shares_to_sell:.4f} shares | "
                f"remaining={position.shares:.4f} | exit={exit_price:.4f} | "
                f"pnl={realised_pnl:+.4f}"
            )

        return True

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
        loop         = asyncio.get_running_loop()
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
                        else (best_bid or best_ask or 0.0)
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

    async def _reconcile_ws_pending(self, raw_by_wallet: Dict[str, Optional[list]]):
        loop = asyncio.get_running_loop()
        for pos_key, pending in self.pending.items():
            if pending.market_id != "pending-ws":
                continue
            wallet_raw = raw_by_wallet.get(pending.source_wallet) or []
            for rest_pos in wallet_raw:
                if rest_pos.get("asset") == pending.token_id:
                    market_id = rest_pos.get("conditionId", "unknown")
                    # Offload blocking HTTP call (#10)
                    question  = await loop.run_in_executor(
                        None, self.get_market_question, market_id
                    )
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
                    if self.dry_run:
                        self.balance.apply_dry_run_cancel(p.size_usd)
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
                    source_shares = 0.0,   # populated on the next REST poll
                )
                del self.pending[k]

    async def _drain_ws_price_queue(self):
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

    async def scan_and_copy(self):
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until:
            return

        is_broken, dd_pct = self.balance.check_drawdown()
        if is_broken is None:
            # Balance unknown — refuse to poll rather than silently proceeding (#5)
            logging.warning("[REST] Balance unknown — skipping poll cycle until balance is confirmed.")
            return
        if is_broken:
            logging.critical(f"🛑 DRAWDOWN TRIGGERED ({dd_pct*100:.1f}%) — pausing 48 h.")
            cfg.bot_paused_until = datetime.now() + timedelta(hours=48)
            return

        current_bal = self.balance.get_balance()
        if current_bal is None:
            return

        self.clean_expired_limit_orders()
        self.process_pending_fills()

        await self._drain_ws_price_queue()

        logging.info(
            f"Poll | Balance: ${current_bal:.2f} | "
            f"Positions: {len(self.positions)} | "
            f"Pending: {len(self.pending)} | "
            f"WS tokens: {len(self._ws_tracked)}"
        )

        all_wallet_data = await self._fetch_all_wallets()

        await self._reconcile_ws_pending(all_wallet_data)  # now async (#10)

        loop = asyncio.get_running_loop()

        for wallet_addr, config in cfg.WALLETS.items():
            copy_mode = config.get("copy_mode", "new_only")
            if copy_mode != "new_only":
                logging.warning(f"[REST] {config['name']} copy_mode='{copy_mode}' not supported — skipping.")
                continue

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

            if wallet_addr not in self._first_scan_done:
                pre_existing = []
                for pos in raw:
                    asset = pos.get("asset")
                    size  = float(pos.get("size", pos.get("shares", 0)))
                    if not asset or size <= 0:
                        continue
                    # Use the same fallback as the scan loop so keys always match.
                    raw_side = (pos.get("outcome") or pos.get("side") or "YES").upper()
                    pre_existing.append(
                        f"{wallet_addr.lower()}_{asset}_{raw_side}"
                    )
                self.seen.snapshot_existing(pre_existing)
                self._first_scan_done.add(wallet_addr)

            for pos in raw:
                token_id  = pos.get("asset")
                shares    = float(pos.get("size", pos.get("shares", 0)))
                side      = (pos.get("outcome") or pos.get("side") or "YES").upper()
                market_id = pos.get("conditionId", "unknown")

                if not token_id or shares <= 0:
                    continue

                pos_key = f"{wallet_addr.lower()}_{token_id}_{side}"

                # Fetch orderbook BEFORE acquiring the lock so we don't hold
                # the lock during a blocking HTTP call.
                best_ask, mid_price = await loop.run_in_executor(
                    None, self.get_orderbook_prices, token_id
                )

                # Acquire lock before the seen-check so a concurrent WS signal
                # for the same pos_key cannot also slip through (#14).
                async with self._pending_lock:
                    if self.seen.is_seen(pos_key) or pos_key in self.pending:
                        continue

                    if len(self.positions) + len(self.pending) >= MAX_POSITIONS:
                        logging.warning(f"[REST] Position limit reached — skipping REST fallback.")
                        continue

                    source_price = float(pos.get("avgPrice", pos.get("price", 0.0)))
                    if source_price > 0:
                        # Use the actual price the source paid as reference
                        if best_ask > 0:
                            premium      = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
                            actual_price = min(best_ask, source_price * (1.0 + premium))
                        else:
                            # Orderbook failed — use source price with small premium
                            premium      = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
                            actual_price = source_price * (1.0 + premium)
                    else:
                        if best_ask <= 0:
                            actual_price = mid_price
                        else:
                            premium      = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
                            actual_price = min(best_ask, mid_price * (1.0 + premium))

                    if actual_price <= 0 or actual_price >= 1.0:
                        logging.error(f"[REST] Invalid price {actual_price} — skipping.")
                        continue

                    # Guard: if we used source price but market is now way higher, skip
                    if source_price > 0 and actual_price > source_price * 1.50:
                        logging.warning(
                            f"[REST] Market moved too far from source price "
                            f"(source={source_price:.4f}, market={actual_price:.4f}) — skipping."
                        )
                        continue

                    source_value = float(pos.get("initialValue", pos.get("value", 0.0)))
                    my_size = _calc_size(config, actual_price, source_value)

                    current_bal = self.balance.get_balance()
                    if current_bal is not None and my_size > current_bal:
                        logging.warning(f"[REST] Order size ${my_size:.2f} exceeds balance ${current_bal:.2f} — skipping.")
                        continue

                    # Offload blocking HTTP call (#11)
                    question_str = await loop.run_in_executor(
                        None, self.get_market_question, market_id
                    )
                    logging.info(
                        f"🔁 [REST FALLBACK] {config['name']} | {side} | "
                        f"'{question_str[:40]}' @ {actual_price:.4f} "
                        f"[signal_source=rest]"
                    )

                    ok, order_id, _ = self.executor.place_limit_buy(token_id, my_size, actual_price)
                    if ok:
                        if self.dry_run:
                            self.balance.apply_dry_run_buy(my_size)

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

            # ── Build a shares map from the REST response ──────────────────────
            # Maps token_id → current source-wallet share count (0 if closed).
            source_shares_map: Dict[str, float] = {
                pos.get("asset"): float(pos.get("size", pos.get("shares", 0)))
                for pos in raw
                if pos.get("asset")
            }

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

            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr:
                    continue
                if position.status != "open":
                    continue

                current_source_shares = source_shares_map.get(position.token_id, 0.0)

                # ── Update source_shares baseline on first poll after fill ──
                if position.source_shares <= 0 and current_source_shares > 0:
                    position.source_shares = current_source_shares
                    logging.debug(
                        f"[REST] source_shares initialised for {pos_key}: "
                        f"{current_source_shares:.4f}"
                    )

                # ── Full exit: source closed the position entirely ───────────
                if position.token_id not in source_token_ids:
                    logging.info(
                        f"📉 [REST EXIT] {position.source_name} fully closed — "
                        f"mirroring full sell [signal={position.signal_source}]"
                    )
                    await self._execute_sell(
                        pos_key         = pos_key,
                        position        = position,
                        shares_to_sell  = position.shares,
                        reference_price = position.current_price or position.entry_price,
                        trigger         = "[REST EXIT]",
                    )
                    continue

                # ── Partial sell: source reduced shares ──────────────────────
                prev_shares = position.source_shares
                if prev_shares > 0 and current_source_shares < prev_shares:
                    reduction = prev_shares - current_source_shares
                    fraction  = reduction / prev_shares

                    # Accumulate sub-threshold reductions so a series of small
                    # cuts that together exceed the threshold still triggers a sell.
                    position.pending_reduction += fraction
                    effective_fraction = position.pending_reduction

                    if effective_fraction >= PARTIAL_SELL_THRESHOLD:
                        dedup_key = f"{pos_key}_{effective_fraction:.2f}"
                        if dedup_key in self._ws_sell_executed:
                            logging.debug(
                                f"[REST] Partial sell for {pos_key} already handled "
                                f"by WS ({effective_fraction:.1%}) — skipping REST duplicate."
                            )
                            position.source_shares     = current_source_shares
                            position.pending_reduction = 0.0
                            continue

                        our_shares_to_sell = round(position.shares * effective_fraction, 4)
                        logging.info(
                            f"✂️  [REST PARTIAL] {position.source_name} reduced "
                            f"{effective_fraction:.1%} of position (accumulated) — selling "
                            f"{our_shares_to_sell:.4f} of our "
                            f"{position.shares:.4f} shares"
                        )
                        sold_ok = await self._execute_sell(
                            pos_key         = pos_key,
                            position        = position,
                            shares_to_sell  = our_shares_to_sell,
                            reference_price = position.current_price or position.entry_price,
                            trigger         = "[REST PARTIAL]",
                        )
                        if sold_ok:
                            position.pending_reduction = 0.0
                    else:
                        logging.info(
                            f"[REST PARTIAL] {position.source_name} reduced {fraction:.1%} "
                            f"(accumulated={effective_fraction:.1%}) — below threshold, accumulating."
                        )

                    # Update baseline regardless of whether we acted.
                    position.source_shares = current_source_shares

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
    drawdown  = min(((cfg.peak_bankroll - bankroll) / cfg.peak_bankroll * 100), 100.0) if cfg.peak_bankroll > 0 else 0.0
    is_paused = bool(cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until)

    status_label = "Paused" if is_paused else "Running"
    status_badge = "badge-paused" if is_paused else "badge-live"
    mode_label   = "Dry Run" if bot.dry_run else "Live"
    mode_badge   = "badge-dry" if bot.dry_run else "badge-live"

    positions_snapshot = list(bot.positions.values())
    closed_list = list(getattr(bot, "closed_positions", []))

    unrealised = 0.0
    pos_rows   = ""
    for p in positions_snapshot:
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

    realised    = sum(p.pnl for p in closed_list)
    closed_rows = ""
    for p in reversed(closed_list):
        outcome_cls = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        pnl_str     = f"{_sign(p.pnl)}${abs(p.pnl):.2f}"
        _src_name   = html.escape(p.source_name)
        _question   = html.escape(p.question[:55])
        closed_rows += f"""
        <tr>
            <td><span class="source-tag">{_src_name}</span>&nbsp;{_signal_badge(p.signal_source)}</td>
            <td class="market-name">{_question}</td>
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
    # comp_delta: growth of the compounding bankroll relative to the peak bankroll
    # baseline. Using cfg.peak_bankroll (updated on every new high-water mark) gives
    # a meaningful green/red signal. The fallback uses the current balance so we
    # never divide by zero or produce a spuriously large positive delta (#13).
    _peak_ref  = cfg.peak_bankroll if cfg.peak_bankroll > 0 else (bankroll or 1.0)
    comp_delta = cfg.compounding_bankroll - _peak_ref

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