#!/usr/bin/env python3
import json
import logging
import asyncio
import os
import http.server
import socketserver
from typing import Dict, List, Optional, Set
import config as cfg

# Synchronized exchange contract definitions
from exchange import (
    RobustBalanceManager, 
    PolymarketExecutor, 
    PolymarketWSListener, 
    PolymarketUserChannelListener
)

from models import Position, SeenTradesStore, save_bankroll, load_bankroll

class BalanceManagerProxy:
    """Internal orchestration abstraction anchoring state queries cleanly."""
    def __init__(self, manager: RobustBalanceManager):
        self.manager = manager

    def get_available_balance(self) -> float:
        return self.manager.get_available_balance()


class CopyTrader:
    """
    PolyGun-Optimized Copy Trading Engine core.
    Offloads active real-time executions entirely to high-speed WebSocket consumers
    while keeping track of safe bookkeeping and position states.
    """
    def __init__(self):
        logging.info("⚙️ Instantiating PolyGun-Optimized CopyTrader Core Context...")
        
        # Pull parameters safely from configuration bindings
        self.dry_run = getattr(cfg, "DRY_RUN", True)
        
        # Instantiate dependencies matching expected exchange parameters perfectly
        self.balance_manager = RobustBalanceManager(dry_run=self.dry_run)
        self.executor = PolymarketExecutor()
        
        # Internal tracking structures
        self.balance_manager_proxy = BalanceManagerProxy(self.balance_manager)
        self.active_positions: Dict[str, Position] = {}

        # SeenTradesStore: uses Postgres when DATABASE_URL is set, local file otherwise
        self.seen_trades = SeenTradesStore(
            filepath=cfg.SEEN_TRADES_FILE,
            db_url=getattr(cfg, "DATABASE_URL", ""),
        )

        # Restore compounding bankroll from Postgres if available
        self._db_conn = self.seen_trades._conn
        if self._db_conn:
            restored = load_bankroll(self._db_conn)
            if restored is not None:
                self.balance_manager._virtual_pnl_delta = restored - getattr(cfg, "INITIAL_BANKROLL", 100.0)
                cfg.compounding_bankroll = restored
                cfg.peak_bankroll = restored
                logging.info(f"💾 Restored compounding bankroll from Postgres: ${restored:.2f}")

    def record_buy(self, token_id: str, wallet: str, wallet_name: str,
                   price: float, size_usd: float, order_id: str) -> Position:
        """
        Creates and stores a Position when a BUY executes.
        Uses token_id + source_wallet as the position key.
        """
        pos_key = f"{token_id}:{wallet}"
        shares = (size_usd / price) if price > 0 else 0.0
        pos = Position(
            market_id=token_id,       # refined to actual market_id when REST data is available
            question="",
            outcome="",
            token_id=token_id,
            entry_price=price,
            size_usd=size_usd,
            shares=shares,
            source_wallet=wallet,
            source_name=wallet_name,
            status="open",
            order_id=order_id,
            signal_source="ws",
        )
        self.active_positions[pos_key] = pos
        self.seen_trades.mark_seen(pos_key)
        logging.info(
            f"📂 [POSITION-OPEN] {wallet_name} | {token_id} | "
            f"{shares:.4f} shares @ ${price:.4f} | key={pos_key}"
        )
        return pos

    def record_sell(self, token_id: str, wallet: str, price: float, shares: float) -> Optional[Position]:
        """
        Closes or partially reduces a tracked Position when a SELL executes.
        Returns the updated Position, or None if no matching position was found.
        """
        pos_key = f"{token_id}:{wallet}"
        pos = self.active_positions.get(pos_key)
        if not pos:
            logging.warning(f"[POSITION] SELL received for untracked position key={pos_key}")
            return None

        payout = shares * price
        pos.exit_price = price
        pos.pnl += payout - (shares * pos.entry_price)

        if shares >= pos.shares:
            pos.status = "closed"
            del self.active_positions[pos_key]
            self.seen_trades.unmark_seen(pos_key)
            logging.info(
                f"📂 [POSITION-CLOSE] {pos.source_name} | {token_id} | "
                f"PnL=${pos.pnl:+.2f} | key={pos_key}"
            )
        else:
            pos.shares -= shares
            logging.info(
                f"📂 [POSITION-PARTIAL-SELL] {pos.source_name} | {token_id} | "
                f"sold {shares:.4f}, remaining {pos.shares:.4f} | key={pos_key}"
            )

        # Persist updated bankroll after every closed/partial position
        if self._db_conn:
            save_bankroll(self._db_conn, cfg.compounding_bankroll)

        return pos

    async def scan_and_copy(self):
        """
        Passive Bookkeeping & Reconciliation Handler.
        Fetches the real live balance from the exchange and updates peak_bankroll.
        """
        logging.info("🔍 Running background reconciliation pass...")

        # Call fetch_with_retry directly — no dummy callback needed
        await self.balance_manager.update_balance_cache(
            self.balance_manager.fetch_with_retry
        )
        current_balance = self.balance_manager.get_available_balance()

        # Keep peak_bankroll current for drawdown calculations
        if current_balance > cfg.peak_bankroll:
            cfg.peak_bankroll = current_balance
            logging.info(f"🏔️  New peak bankroll recorded: ${cfg.peak_bankroll:.2f}")

        logging.info(
            f"📊 [RECONCILE] Balance: ${current_balance:.2f} | "
            f"Peak: ${cfg.peak_bankroll:.2f} | "
            f"Open Positions: {len(self.active_positions)} | "
            f"Tracking {len(cfg.WALLETS)} wallets"
        )


class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Forces HTTP server to allocate independent threads per telemetry request."""
    daemon_threads = True


def run_health_server():
    """
    Spins up an isolated operational dashboard telemetry listener.
    Runs bounded safely inside its own parallel daemon hardware thread wrapper context.
    """
    class HealthHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            live_balance = getattr(cfg, "compounding_bankroll", 0.0)
            peak         = getattr(cfg, "peak_bankroll", 0.0)
            drawdown     = ((peak - live_balance) / peak) if peak > 0 else 0.0

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            
            response = {
                "status": "HEALTHY",
                "engine": "POLYGON_OPTIMIZED_V2",
                "pipeline": "EVENT_DRIVEN_QUEUE",
                "account_balance": live_balance,
                "peak_bankroll": peak,
                "current_drawdown_pct": round(drawdown * 100, 2),
                "strict_mode": getattr(cfg, "STRICT_PRICE_MATCH", True),
            }
            self.wfile.write(json.dumps(response).encode("utf-8"))

        def log_message(self, format, *args):
            return

    port = getattr(cfg, "HEALTH_PORT", 8080)
    logging.info(f"🌐 Activating container health telemetry layer binding on port: {port}")
    
    try:
        server_address = ("0.0.0.0", port)
        ThreadingHTTPServer.allow_reuse_address = True
        httpd = ThreadingHTTPServer(server_address, HealthHandler)
        httpd.serve_forever()
    except Exception as server_err:
        logging.critical(f"❌ Failed to initialize dashboard listener loop: {server_err}")
