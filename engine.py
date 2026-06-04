#!/usr/bin/env python3
import json
import logging
import asyncio
import os
import http.server
import socketserver
from typing import Dict, List, Optional
import config as cfg

# Synchronized exchange contract definitions
from exchange import (
    RobustBalanceManager, 
    PolymarketExecutor, 
    PolymarketWSListener, 
    PolymarketUserChannelListener
)

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
        
        # Internal tracking structures to preserve alignment with bot.py
        self.balance_manager_proxy = BalanceManagerProxy(self.balance_manager)
        self.active_positions: Dict[str, dict] = {}
        self.seen_trades: Set[str] = self._load_seen_trades()

    def _load_seen_trades(self) -> set:
        """Loads transaction hashes or unique identifiers to prevent double execution."""
        if os.path.exists(cfg.SEEN_TRADES_FILE):
            try:
                with open(cfg.SEEN_TRADES_FILE, "r") as f:
                    return set(json.load(f))
            except Exception as e:
                logging.warning(f"Failed to read seen trades file: {e}")
        return set()

    def _save_seen_trades(self):
        """Saves processed transaction boundaries out to disk storage safely."""
        try:
            with open(cfg.SEEN_TRADES_FILE, "w") as f:
                json.dump(list(self.seen_trades), f)
        except Exception as e:
            logging.error(f"Failed to persist seen trades boundary map: {e}")

    async def scan_and_copy(self):
        """
        Passive Bookkeeping & Reconciliation Handler.
        Bypasses active trade generation to keep portfolio state aligned 
        without duplicating high-speed WebSocket executions.
        """
        logging.info("🔍 Running background reconciliation pass...")
        
        # Update internal balance caches without hitting aggressive loop locks
        async def dummy_fetch():
            return getattr(cfg, "compounding_bankroll", 100.0)
        
        await self.balance_manager.update_balance_cache(dummy_fetch)
        current_balance = self.balance_manager.get_available_balance()
        
        logging.info(
            f"📊 [RECONCILE] Safe Balance Level: ${current_balance:.2f} | "
            f"Active Tracking Target Rosters: {len(cfg.WALLETS)}"
        )
        return


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
            # Dynamic calculation to avoid throwing stale values
            live_balance = getattr(cfg, "compounding_bankroll", 0.0)
            
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")  # Eliminate cross-origin browser blocking
            self.end_headers()
            
            response = {
                "status": "HEALTHY",
                "engine": "POLYGON_OPTIMIZED_V2",
                "pipeline": "EVENT_DRIVEN_QUEUE",
                "account_balance": live_balance,
                "strict_mode": getattr(cfg, "STRICT_PRICE_MATCH", True)
            }
            self.wfile.write(json.dumps(response).encode("utf-8"))

        def log_message(self, format, *args):
            # Suppress noisy container health check routing logs
            return

    port = getattr(cfg, "HEALTH_PORT", 8080)
    logging.info(f"🌐 Activating container health telemetry layer binding on port: {port}")
    
    try:
        # Binding to 0.0.0.0 is critical for Render/Railway/Docker container routing
        server_address = ("0.0.0.0", port)
        ThreadingHTTPServer.allow_reuse_address = True
        httpd = ThreadingHTTPServer(server_address, HealthHandler)
        httpd.serve_forever()
    except Exception as server_err:
        logging.critical(f"❌ Failed to initialize dashboard listener loop: {server_err}")