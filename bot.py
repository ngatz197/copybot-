#!/usr/bin/env python3
"""
main.py - Strategy Engine
- Orchestrates async run cycles and processes API streams for target open tracking.
- Implements Dynamic Position Sizing and Drawdown circuit breaker algorithms.
- Orchestrates multi-file structure bindings and initial execution sequences.
"""

import asyncio
import logging
import threading
import time
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, Set
from http.server import HTTPServer

import config as cfg
from services import SeenTradesStore, RobustBalanceManager, PolymarketExecutor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

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


class CopyTrader:
    def __init__(self):
        self.dry_run    = cfg.DRY_RUN
        self.balance    = RobustBalanceManager()
        self.executor   = PolymarketExecutor(cfg.DRY_RUN)
        self.seen       = SeenTradesStore(cfg.SEEN_TRADES_FILE, cfg.DATABASE_URL)
        self.positions: Dict[str, Position]        = {}
        self.pending:   Dict[str, PendingLimitBuy] = {}
        self.closed_positions = []
        self._first_scan_done: Set[str] = set()

    def get_orderbook_prices(self, token_id: str) -> tuple:
        for attempt in range(cfg.MAX_RETRIES):
            try:
                r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8)
                if r.status_code == 200:
                    bids = r.json().get("bids", [])
                    asks = r.json().get("asks", [])
                    best_bid = float(bids[0]["price"]) if bids else 0.0
                    best_ask = float(asks[0]["price"]) if asks else 0.0
                    mid = (best_bid + best_ask) / 2 if best_bid and best_ask else best_bid or best_ask
                    return mid, best_ask
            except Exception:
                time.sleep(cfg.RETRY_DELAY)
        return 0.0, 0.0

    def get_risk_percent(self, price: float, config: dict) -> float:
        if config.get("risk_type") == "fixed":
            return config.get("fixed_risk", 0.025)
        return 0.03 if price >= 0.70 else (0.01 if price >= 0.30 else 0.006)

    def check_drawdown(self) -> bool:
        current = self.balance.get_balance()
        if current is None: return False
        if current > cfg.peak_bankroll:
            cfg.peak_bankroll = current
        dd = (cfg.peak_bankroll - current) / cfg.peak_bankroll if cfg.peak_bankroll > 0 else 0
        if dd >= cfg.MAX_DRAWDOWN:
            if cfg.bot_paused_until is None or datetime.now() > cfg.bot_paused_until:
                cfg.bot_paused_until = datetime.now() + timedelta(hours=cfg.PAUSE_HOURS)
                logging.warning(f"CRITICAL: Drawdown threshold breached ({dd*100:.1f}%). Halting operations.")
            return True
        return False

    def _get_positions(self, wallet_addr: str) -> list | None:
        try:
            r = requests.get(f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50", timeout=12)
            if r.status_code == 200: return r.json()
        except Exception:
            pass
        return None

    def _process_pending_orders(self, source_token_ids_by_wallet: dict):
        for pos_key, pending in list(self.pending.items()):
            wallet_tokens = source_token_ids_by_wallet.get(pending.source_wallet, set())

            if pending.token_id not in wallet_tokens:
                self.executor.cancel_order(pending.order_id)
                del self.pending[pos_key]
                continue

            if self.executor.is_order_filled(pending.order_id):
                shares = pending.size_usd / pending.limit_price if pending.limit_price > 0 else 0
                self.positions[pos_key] = Position(
                    market_id=pending.market_id, question=pending.question, outcome=pending.outcome,
                    token_id=pending.token_id, entry_price=pending.limit_price, size_usd=pending.size_usd,
                    shares=shares, source_wallet=pending.source_wallet, source_name=pending.source_name, order_id=pending.order_id
                )
                del self.pending[pos_key]
                continue

            if (datetime.now() - pending.placed_at).total_seconds() >= cfg.LIMIT_EXPIRY_SECONDS:
                self.executor.cancel_order(pending.order_id)
                del self.pending[pos_key]

                mid, ask = self.get_orderbook_prices(pending.token_id)
                current_ask = ask if ask > 0 else mid
                if current_ask <= 0: continue

                _cfg = cfg.WALLETS.get(pending.source_wallet, {})
                premium = _cfg.get("limit_buy_max_premium", cfg.LIMIT_BUY_MAX_PREMIUM)
                limit_price = round(min(current_ask, current_ask * (1 + premium)), 4)

                ok, order_id, filled_price = self.executor.place_limit_buy(pending.token_id, pending.size_usd, limit_price)
                if ok:
                    self.pending[pos_key] = PendingLimitBuy(
                        pos_key=pos_key, token_id=pending.token_id, market_id=pending.market_id,
                        question=pending.question, outcome=pending.outcome, source_wallet=pending.source_wallet,
                        source_name=pending.source_name, limit_price=filled_price, size_usd=pending.size_usd, order_id=order_id
                    )

    async def scan_and_copy(self):
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until: return
        if self.check_drawdown(): return

        current_bal = self.balance.get_balance()
        if current_bal is None: return

        logging.info(f"Scan Run | Net Assets: ${current_bal:.2f} pUSD | Active Orders: {len(self.positions)} | Open Limits: {len(self.pending)}")
        source_token_ids_by_wallet = {}

        import requests # Fallback check within async scope loop thread context

        for wallet_addr, config in cfg.WALLETS.items():
            raw = self._get_positions(wallet_addr)
            if raw is None: continue

            source_token_ids = {pos.get("asset") for pos in raw if pos.get("asset") and float(pos.get("size", pos.get("shares", 0))) > 0}

            if wallet_addr not in self._first_scan_done:
                self._first_scan_done.add(wallet_addr)
                if config.get("copy_mode") == "new_only":
                    self.seen.snapshot_existing({f"{wallet_addr}_{tid}" for tid in source_token_ids})
                    source_token_ids_by_wallet[wallet_addr] = source_token_ids
                    continue

            for pos in raw:
                token_id  = pos.get("asset", "")
                market_id = pos.get("conditionId", "")
                question  = pos.get("title", "Unknown")
                outcome   = pos.get("outcome", "YES")
                size_usd  = float(pos.get("currentValue", 0))

                min_val = 0.0 if config.get("copy_sub_dollar") else 1.0
                if not token_id or size_usd < min_val or size_usd <= 0: continue

                pos_key = f"{wallet_addr}_{token_id}"
                if self.seen.is_seen(pos_key) or pos_key in self.positions or pos_key in self.pending: continue
                if len(self.positions) + len(self.pending) >= cfg.MAX_POSITIONS: break

                cur_price = float(pos.get("curPrice", 0))
                if cur_price <= 0: continue

                limit_price = round(cur_price, 4)
                if config.get("copy_sub_dollar") and size_usd < 1.0:
                    my_size = round(size_usd, 2)
                else:
                    my_size = round(cfg.compounding_bankroll * self.get_risk_percent(limit_price, config), 2)

                ok, order_id, actual_price = self.executor.place_limit_buy(token_id, my_size, limit_price)
                if ok:
                    self.seen.mark_seen(pos_key)
                    self.pending[pos_key] = PendingLimitBuy(
                        pos_key=pos_key, token_id=token_id, market_id=market_id, question=question,
                        outcome=outcome, source_wallet=wallet_addr, source_name=config["name"],
                        limit_price=actual_price, size_usd=my_size, order_id=order_id
                    )

            source_token_ids_by_wallet[wallet_addr] = source_token_ids

            cur_price_map = {pos.get("asset"): float(pos.get("curPrice", 0)) for pos in raw if pos.get("asset") and float(pos.get("curPrice", 0)) > 0}
            for _pk, _pos in self.positions.items():
                if _pos.source_wallet == wallet_addr and _pos.token_id in cur_price_map:
                    _pos.current_price = cur_price_map[_pos.token_id]

            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr: continue
                if position.token_id not in source_token_ids and position.status == "open":
                    exit_price, _ = self.get_orderbook_prices(position.token_id)
                    ok, _ = self.executor.place_sell(position.token_id, position.shares)
                    if ok:
                        pnl = (exit_price - position.entry_price) * position.shares
                        position.status, position.exit_price, position.pnl = "closed", exit_price, pnl
                        if pnl > 0:
                            cfg.compounding_bankroll += pnl * cfg.COMPOUNDING_RATE
                        self.closed_positions.append(position)
                        del self.positions[pos_key]

        self._process_pending_orders(source_token_ids_by_wallet)

    async def run(self):
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Runtime execution error: {e}")
            await asyncio.sleep(cfg.POLL_INTERVAL)


def run_health_server():
    server = HTTPServer(("0.0.0.0", cfg.HEALTH_PORT), cfg.HealthHandler)
    server.serve_forever()

async def main():
    threading.Thread(target=run_health_server, daemon=True).start()

    bot = CopyTrader()
    cfg._bot_ref = bot

    try:
        start_bal = bot.balance.fetch_with_retry(retries=5, delay=10)
        cfg.peak_bankroll = start_bal
        cfg.compounding_bankroll = start_bal
        logging.info(f"System boot complete. Compounding pool initialized: ${start_bal:.2f} pUSD")
    except Exception as e:
        logging.error(f"Degraded Boot Mode active: balance fetch timeout. {e}")

    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())