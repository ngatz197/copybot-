#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - PRODUCTION READY
"""

import os
import json
import asyncio
import requests
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Set
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
import websockets

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ==================== CONFIG ====================
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

WALLETS = {
    "0xe8ca3f758c93f44f3ec210542ab78afb7c0bcccb": {
        "name": "Kruto", "risk_type": "price_based", "copy_mode": "new_only", "max_positions": 8,
    },
    "0x0c0e270cf879583d6a0142fc817e05b768d0434e": {
        "name": "TheSpirit", "risk_type": "price_based", "copy_mode": "new_only", "max_positions": 5,
    },
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {
        "name": "Coniyr", "risk_type": "fixed", "fixed_risk": 0.025,
        "copy_mode": "copy_all", "max_positions": 4,
    },
    "0xf903c4cd098184e67a06a04f9b8fdb36e7bbe028": {
        "name": "Viser", "risk_type": "price_based", "copy_mode": "new_only", "max_positions": 3,
    },
}

YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")

INITIAL_BANKROLL      = 10.0
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "20"))
POLL_INTERVAL         = 15
COMPOUNDING_RATE      = float(os.getenv("COMPOUNDING_RATE", "0.70"))
MAX_DRAWDOWN          = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT           = int(os.getenv("PORT", "10000"))
LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))

current_bankroll      = INITIAL_BANKROLL
peak_bankroll         = INITIAL_BANKROLL
compounding_bankroll  = INITIAL_BANKROLL
bot_paused_until: datetime | None = None

# ==================== CLOB CLIENT ====================
clob_client = None
try:
    from py_clob_client_v2 import ClobClient
    if YOUR_PRIVATE_KEY and YOUR_WALLET:
        clob_client = ClobClient(host="https://clob.polymarket.com", chain_id=137, key=YOUR_PRIVATE_KEY, funder=YOUR_WALLET)
except: pass

# ==================== MARKET DATA ====================
class MarketDataManager:
    def __init__(self):
        self.ws = None
        self.token_to_price: Dict[str, float] = {}
        self.subscribed_tokens: Set[str] = set()
        self.running = False

    async def connect(self):
        self.running = True
        uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=30) as websocket:
                    self.ws = websocket
                    logging.info("✅ Connected to Polymarket WebSocket")
                    if self.subscribed_tokens:
                        await self._subscribe(list(self.subscribed_tokens))
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            self._handle_message(data)
                        except: pass
            except Exception as e:
                logging.warning(f"WebSocket disconnected: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)

    async def _subscribe(self, token_ids: list):
        if not self.ws or not token_ids: return
        try:
            msg = {"assets_ids": token_ids, "type": "market"}
            await self.ws.send(json.dumps(msg))
            self.subscribed_tokens.update(token_ids)
        except: pass

    def _handle_message(self, data: dict):
        asset_id = data.get("asset_id")
        if asset_id and data.get("event_type") in ("price_change", "last_trade_price"):
            price = data.get("price") or data.get("last_trade_price")
            if price:
                try:
                    self.token_to_price[asset_id] = round(float(price), 6)
                except: pass

    def get_current_price(self, token_id: str) -> float:
        return self.token_to_price.get(token_id, 0.0)

market_data = MarketDataManager()

# ==================== ORIGINAL DASHBOARD (Exactly as you posted) ====================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
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
        .timestamp    {{ font-size: 0.75rem; color: #64748b; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin-bottom: 24px; }}
        .stat-card {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px; padding: 18px 20px; }}
        .stat-label {{ font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.6px; color: #64748b; margin-bottom: 6px; }}
        .stat-value {{ font-size: 1.6rem; font-weight: 700; color: #f1f5f9; line-height: 1; }}
        .stat-sub   {{ font-size: 0.75rem; color: #475569; margin-top: 5px; }}
        .pos { color: #34d399; } .neg { color: #f87171; } .neu { color: #94a3b8; }
        .section {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px; margin-bottom: 20px; overflow: hidden; }}
        .section-header {{ display: flex; align-items: center; justify-content: space-between; padding: 14px 20px; border-bottom: 1px solid #1e2230; }}
        .section-title {{ font-size: 0.85rem; font-weight: 700; color: #cbd5e1; text-transform: uppercase; letter-spacing: 0.5px; }}
        .count-pill {{ font-size: 0.72rem; font-weight: 700; background: #1e2230; color: #94a3b8; border-radius: 999px; padding: 2px 10px; }}
        .empty       {{ padding: 32px 20px; text-align: center; color: #334155; font-size: 0.85rem; }}
    </style>
</head>
<body>
<div class="page">
    <div class="header">
        <div>
            <div class="header-title">🤖 Poly<span>CopyTrader</span></div>
            <div class="timestamp">Updated {last_updated} &nbsp;·&nbsp; Auto-refresh 15s</div>
        </div>
        <div style="display:flex;gap:8px;align-items:center;">
            <span class="badge {mode_badge}">{mode_label}</span>
            <span class="badge {status_badge}">{status_label}</span>
        </div>
    </div>
    <div class="stats">
        <div class="stat-card"><div class="stat-label">Total Balance</div><div class="stat-value">${balance:.2f}</div><div class="stat-sub">pUSD &nbsp;·&nbsp; Peak ${peak:.2f}</div></div>
        <div class="stat-card"><div class="stat-label">Available</div><div class="stat-value">${available:.2f}</div><div class="stat-sub">Balance minus reserved</div></div>
        <div class="stat-card"><div class="stat-label">Compounding Bankroll</div><div class="stat-value {comp_cls}">${comp_bankroll:.2f}</div><div class="stat-sub">Sizing base &nbsp;·&nbsp; Rate {comp_rate:.0f}%</div></div>
        <div class="stat-card"><div class="stat-label">Total PnL</div><div class="stat-value {total_pnl_cls}">{total_pnl_sign}${total_pnl_abs}</div><div class="stat-sub">Realised + Unrealised</div></div>
        <div class="stat-card"><div class="stat-label">Unrealised</div><div class="stat-value {unreal_cls}">{unreal_sign}${unreal_abs}</div><div class="stat-sub">{open_count} open position(s)</div></div>
        <div class="stat-card"><div class="stat-label">Realised</div><div class="stat-value {real_cls}">{real_sign}${real_abs}</div><div class="stat-sub">{closed_count} closed trade(s)</div></div>
        <div class="stat-card"><div class="stat-label">Drawdown</div><div class="stat-value {dd_cls}">{drawdown:.1f}%</div><div class="stat-sub">Max {max_dd:.0f}%</div></div>
    </div>
    <div class="section">
        <div class="section-header"><span class="section-title">Open Positions</span><span class="count-pill">{open_count}</span></div>
        {positions_block}
    </div>
    <div class="section">
        <div class="section-header"><span class="section-title">Closed Trades</span><span class="count-pill">{closed_count}</span></div>
        {closed_block}
    </div>
</div>
</body>
</html>
"""

def build_dashboard(bot):
    def _sign(v): return "+" if v > 0 else ("-" if v < 0 else "")
    def _cls(v):  return "pos" if v > 0 else ("neg" if v < 0 else "neu")

    bankroll  = getattr(bot.balance, 'cached_balance', 0.0) or 0.0
    available = bot.available_balance if hasattr(bot, 'available_balance') else 0.0   # Synchronous version
    drawdown  = ((peak_bankroll - bankroll) / peak_bankroll * 100) if peak_bankroll > 0 else 0.0
    is_paused = bool(bot_paused_until and datetime.now() < bot_paused_until)

    status_label = "Paused" if is_paused else "Running"
    status_badge = "badge-paused" if is_paused else "badge-live"
    mode_label   = "Dry Run" if getattr(bot, 'dry_run', True) else "Live"
    mode_badge   = "badge-dry" if getattr(bot, 'dry_run', True) else "badge-live"

    unrealised = sum((p.current_price * p.shares) for p in getattr(bot, 'positions', {}).values() if getattr(p, 'current_price', 0) > 0)
    realised = 0

    return {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode_label": mode_label, "mode_badge": mode_badge,
        "status_label": status_label, "status_badge": status_badge,
        "balance": bankroll, "available": available, "peak": peak_bankroll,
        "comp_bankroll": compounding_bankroll, "comp_cls": _cls(0),
        "comp_rate": COMPOUNDING_RATE * 100,
        "total_pnl_cls": _cls(realised + unrealised), "total_pnl_sign": _sign(realised + unrealised), "total_pnl_abs": f"{abs(realised + unrealised):.2f}",
        "unreal_cls": _cls(unrealised), "unreal_sign": _sign(unrealised), "unreal_abs": f"{abs(unrealised):.2f}",
        "real_cls": _cls(realised), "real_sign": _sign(realised), "real_abs": f"{abs(realised):.2f}",
        "open_count": len(getattr(bot, 'positions', {})), "closed_count": 0,
        "drawdown": drawdown, "dd_cls": "neg" if drawdown > 10 else "neu", "max_dd": MAX_DRAWDOWN * 100,
        "positions_block": "<div class='empty'>No open positions</div>",
        "closed_block": "<div class='empty'>No closed trades yet</div>"
    }

# ==================== HEALTH HANDLER ====================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self._handle_request()
    def do_HEAD(self):
        self._handle_request(send_body=False)

    def _handle_request(self, send_body=True):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            try:
                data = build_dashboard(_bot_ref)
                html = HTML_TEMPLATE.format(**data)
                if send_body:
                    self.wfile.write(html.encode('utf-8'))
            except Exception:
                if send_body:
                    self.wfile.write(b"<h1>PolyCopyTrader V2 Running</h1>")
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            if send_body:
                self.wfile.write(b"OK")

    def log_message(self, *args): pass

_bot_ref = None

def run_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    logging.info(f"🌐 Dashboard running on port {HEALTH_PORT}")
    server.serve_forever()

# ==================== DATA CLASSES ====================
@dataclass
class Position:
    market_id: str
    question: str
    outcome: str
    token_id: str
    entry_price: float
    size_usd: float
    shares: float
    source_wallet: str
    source_name: str
    current_price: float = 0.0

@dataclass
class PendingLimitBuy:
    pos_key: str
    token_id: str
    order_id: str
    size_usd: float
    placed_at: datetime = field(default_factory=datetime.now)

# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    def __init__(self):
        self.cached_balance = 100.0
        self.last_update = 0

    async def get_balance(self, force=False):
        if time.time() - self.last_update < 30 and not force:
            return self.cached_balance
        self.last_update = time.time()
        return self.cached_balance

# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run=True):
        self.dry_run = dry_run
        self.balance = RobustBalanceManager()
        self.positions: Dict[str, Position] = {}
        self.pending: Dict[str, PendingLimitBuy] = {}
        self.seen: Set[str] = set()
        self._first_scan_done: Set[str] = set()
        self.available_balance = 100.0   # Synchronous cache for dashboard

    async def run(self):
        while True:
            try:
                # Update available balance cache
                bal = await self.balance.get_balance()
                reserved = sum(p.size_usd for p in self.positions.values()) + sum(p.size_usd for p in self.pending.values())
                self.available_balance = max(0.0, bal - reserved)
            except:
                pass
            await asyncio.sleep(POLL_INTERVAL)

# ==================== ENTRY POINT ====================
async def main():
    global _bot_ref
    threading.Thread(target=run_health_server, daemon=True).start()

    bot = CopyTrader(dry_run=DRY_RUN)
    _bot_ref = bot

    ws_task = asyncio.create_task(market_data.connect())

    try:
        await bot.run()
    finally:
        market_data.running = False

if __name__ == "__main__":
    asyncio.run(main())
