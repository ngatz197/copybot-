#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - WEBSOCKET + REST HYBRID
Features:
- WebSocket real-time position updates (<1 second delay)
- REST API polling fallback (if WebSocket fails)
- Cumulative small sell detection
- Partial fill handling
- TTL Caching for all API calls
- Share-based sell filtering (20% threshold)
- 15-second REST polling as backup
"""

import os
import json
import asyncio
import requests
import logging
import time
import threading
import websocket
import ssl
import hmac
import hashlib
import base64
from datetime import datetime, timedelta
from typing import Dict, Set, Tuple, Optional, List, Any
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
from collections import deque

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
MIN_SELL_PERCENT = float(os.getenv("MIN_SELL_PERCENT", "20"))
POLL_INTERVAL = int(os.getenv("POLL_SECONDS", "15"))  # REST fallback interval

# WebSocket Configuration
WEBSOCKET_URL = os.getenv("WEBSOCKET_URL", "wss://ws-subscriptions-clob.polymarket.com/ws")
WEBSOCKET_ENABLED = os.getenv("WEBSOCKET_ENABLED", "true").lower() == "true"
WEBSOCKET_RECONNECT_DELAY = int(os.getenv("WEBSOCKET_RECONNECT_DELAY", "5"))

# Cache TTL settings
ORDERBOOK_CACHE_TTL = int(os.getenv("ORDERBOOK_CACHE_TTL", "3"))
BID_CACHE_TTL = int(os.getenv("BID_CACHE_TTL", "2"))

WALLETS = {
    "0xe8ca3f758c93f44f3ec210542ab78afb7c0bcccb": {
        "name": "Kruto",
        "risk_type": "price_based",
        "copy_mode": "new_only",
        "limit_buy_max_premium": 0.10,
        "copy_sub_dollar": True,
        "max_positions": 8,
    },
    "0x0c0e270cf879583d6a0142fc817e05b768d0434e": {
        "name": "TheSpirit",
        "risk_type": "price_based",
        "copy_mode": "new_only",
        "max_positions": 5,
    },
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {
        "name": "Coniyr",
        "risk_type": "fixed",
        "fixed_risk": 0.025,
        "copy_mode": "copy_all",
        "max_positions": 4,
    },
    "0xf903c4cd098184e67a06a04f9b8fdb36e7bbe028": {
        "name": "Viser",
        "risk_type": "price_based",
        "copy_mode": "new_only",
        "max_positions": 3,
    },
}

YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
POLY_API_KEY = os.getenv("POLY_API_KEY", "")
POLY_SECRET = os.getenv("POLY_SECRET", "")
POLY_PASSPHRASE = os.getenv("POLY_PASSPHRASE", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

INITIAL_BANKROLL = 10.0
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "20"))
COMPOUNDING_RATE = float(os.getenv("COMPOUNDING_RATE", "0.70"))
MAX_DRAWDOWN = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT = int(os.getenv("PORT", "8080"))
PAUSE_HOURS = 48
MAX_RETRIES = 3
RETRY_DELAY = 5
MAX_SLIPPAGE = float(os.getenv("MAX_SLIPPAGE", "0.20"))
LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))
LIMIT_EXPIRY_SECONDS = int(os.getenv("LIMIT_EXPIRY_SECONDS", "300"))
SEEN_TRADES_FILE = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")
MAX_FILL_CHECK_ERRORS = int(os.getenv("MAX_FILL_CHECK_ERRORS", "5"))
PUSD_CONTRACT_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
SELL_SETTLE_WAIT = int(os.getenv("SELL_SETTLE_WAIT", "8"))

current_bankroll = INITIAL_BANKROLL
peak_bankroll = INITIAL_BANKROLL
compounding_bankroll = INITIAL_BANKROLL
bot_paused_until: Optional[datetime] = None
_trade_lock = threading.Lock()

# ==================== TTL CACHE ====================
class TTLCache:
    def __init__(self, default_ttl: int = 5, max_size: int = 500):
        self.cache: Dict[str, Tuple[any, float]] = {}
        self.default_ttl = default_ttl
        self.max_size = max_size
        self._lock = threading.Lock()
        self._hit_count = 0
        self._miss_count = 0
    
    def get(self, key: str) -> Optional[any]:
        with self._lock:
            if key not in self.cache:
                self._miss_count += 1
                return None
            value, timestamp = self.cache[key]
            if time.time() - timestamp > self.default_ttl:
                del self.cache[key]
                self._miss_count += 1
                return None
            self._hit_count += 1
            return value
    
    def set(self, key: str, value: any, ttl: int = None):
        with self._lock:
            self.cache[key] = (value, time.time())
            if len(self.cache) > self.max_size:
                items_to_remove = len(self.cache) - self.max_size
                for _ in range(min(items_to_remove, len(self.cache))):
                    self.cache.popitem(last=False)
    
    def get_stats(self):
        total = self._hit_count + self._miss_count
        hit_rate = (self._hit_count / total * 100) if total > 0 else 0
        return {'hits': self._hit_count, 'misses': self._miss_count, 'hit_rate_percent': round(hit_rate, 1), 'size': len(self.cache)}

orderbook_cache = TTLCache(default_ttl=ORDERBOOK_CACHE_TTL, max_size=500)
bid_cache = TTLCache(default_ttl=BID_CACHE_TTL, max_size=200)

# ==================== ENHANCED POSITION ====================
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
    status: str = "open"
    exit_price: float = 0.0
    pnl: float = 0.0
    order_id: str = ""
    current_price: float = 0.0
    source_shares: float = 0.0
    shares_at_open: float = 0.0
    source_shares_at_open: float = 0.0
    cumulative_sold_percent: float = 0.0
    last_source_shares: float = 0.0

@dataclass
class PendingLimitBuy:
    pos_key: str
    token_id: str
    market_id: str
    question: str
    outcome: str
    source_wallet: str
    source_name: str
    limit_price: float
    size_usd: float
    order_id: str
    source_shares: float = 0.0
    fill_check_errors: int = 0
    placed_at: datetime = field(default_factory=datetime.now)

# ==================== SELL FILTER METRICS ====================
class SellFilterMetrics:
    def __init__(self):
        self.total_sells_considered = 0
        self.sells_executed = 0
        self.sells_skipped = 0
        self.skipped_shares = 0.0
        self.executed_shares = 0.0
        self.total_source_sell_percent = 0.0
        self.cumulative_triggers = 0
        self.ws_updates_received = 0
        self.rest_fallbacks_used = 0
    
    def record_decision(self, executed: bool, shares: float, source_sell_percent: float, is_cumulative: bool = False):
        self.total_sells_considered += 1
        if executed:
            self.sells_executed += 1
            self.executed_shares += shares
            if is_cumulative:
                self.cumulative_triggers += 1
        else:
            self.sells_skipped += 1
            self.skipped_shares += shares
            self.total_source_sell_percent += source_sell_percent
    
    def record_ws_update(self):
        self.ws_updates_received += 1
    
    def record_rest_fallback(self):
        self.rest_fallbacks_used += 1
    
    def get_metrics(self):
        return {
            'total_sells_considered': self.total_sells_considered,
            'sells_executed': self.sells_executed,
            'sells_skipped': self.sells_skipped,
            'skip_rate_percent': round((self.sells_skipped / self.total_sells_considered * 100), 1) if self.total_sells_considered > 0 else 0,
            'skipped_shares': round(self.skipped_shares, 4),
            'executed_shares': round(self.executed_shares, 4),
            'min_sell_percent': MIN_SELL_PERCENT,
            'avg_skipped_sell_percent': round(self.total_source_sell_percent / self.sells_skipped, 1) if self.sells_skipped > 0 else 0,
            'cumulative_triggers': self.cumulative_triggers,
            'ws_updates_received': self.ws_updates_received,
            'rest_fallbacks_used': self.rest_fallbacks_used,
        }

sell_metrics = SellFilterMetrics()

# ==================== CLOB V2 CLIENT ====================
try:
    from py_clob_client_v2 import (
        ClobClient, OrderArgs, MarketOrderArgs, OrderType, Side, ApiCreds, PartialCreateOrderOptions,
    )
    CLOB_AVAILABLE = True
    logger.info("✅ py_clob_client_v2 loaded successfully")
except ImportError:
    CLOB_AVAILABLE = False
    logger.warning("py_clob_client_v2 not installed — running in simulation mode.")

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logger.warning("psycopg2 not installed — seen_trades will fall back to local file.")

# ==================== DASHBOARD ====================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CopyTrader Dashboard - WebSocket Enhanced</title>
    <meta http-equiv="refresh" content="15">
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0d0d0f; color: #e2e8f0; min-height: 100vh; padding: 24px 16px; }
        .page { max-width: 1200px; margin: 0 auto; }
        .header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 28px; flex-wrap: wrap; gap: 8px; }
        .header-title { font-size: 1.25rem; font-weight: 700; color: #f8fafc; }
        .header-title span { color: #6ee7b7; }
        .badge { font-size: 0.72rem; font-weight: 600; padding: 3px 10px; border-radius: 999px; text-transform: uppercase; }
        .badge-live { background: #064e3b; color: #6ee7b7; border: 1px solid #065f46; }
        .badge-dry { background: #1e1b4b; color: #a5b4fc; border: 1px solid #312e81; }
        .badge-ws { background: #1e3a5f; color: #60a5fa; border: 1px solid #3b82f6; }
        .timestamp { font-size: 0.75rem; color: #64748b; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin-bottom: 24px; }
        .stat-card { background: #16181d; border: 1px solid #1e2230; border-radius: 12px; padding: 18px 20px; }
        .stat-label { font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.6px; color: #64748b; margin-bottom: 6px; }
        .stat-value { font-size: 1.6rem; font-weight: 700; color: #f1f5f9; line-height: 1; }
        .stat-sub { font-size: 0.75rem; color: #475569; margin-top: 5px; }
        .pos { color: #34d399; } .neg { color: #f87171; } .neu { color: #94a3b8; }
        .section { background: #16181d; border: 1px solid #1e2230; border-radius: 12px; margin-bottom: 20px; overflow: hidden; }
        .section-header { display: flex; align-items: center; justify-content: space-between; padding: 14px 20px; border-bottom: 1px solid #1e2230; }
        .section-title { font-size: 0.85rem; font-weight: 700; color: #cbd5e1; text-transform: uppercase; letter-spacing: 0.5px; }
        .count-pill { font-size: 0.72rem; font-weight: 700; background: #1e2230; color: #94a3b8; border-radius: 999px; padding: 2px 10px; }
        .tbl-wrap { overflow-x: auto; }
        table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
        thead th { padding: 10px 16px; text-align: left; font-size: 0.70rem; font-weight: 600; text-transform: uppercase; color: #475569; background: #13151a; }
        tbody tr { border-top: 1px solid #1a1d26; }
        tbody tr:hover { background: #1c1f28; }
        tbody td { padding: 12px 16px; color: #cbd5e1; }
        .metric-good { color: #34d399; }
        .metric-bad { color: #f87171; }
        .empty { padding: 32px 20px; text-align: center; color: #334155; }
    </style>
</head>
<body>
<div class="page">
    <div class="header">
        <div>
            <div class="header-title">🤖 Poly<span>CopyTrader</span> <span class="badge badge-ws">WebSocket</span></div>
            <div class="timestamp">Updated {last_updated} &nbsp;·&nbsp; Auto-refresh 15s</div>
        </div>
        <div style="display:flex;gap:8px;align-items:center;">
            <span class="badge {mode_badge}">{mode_label}</span>
            <span class="badge {status_badge}">{status_label}</span>
        </div>
    </div>
    
    <!-- WebSocket Status -->
    <div class="section">
        <div class="section-header">
            <span class="section-title">🔌 WebSocket Status</span>
            <span class="count-pill">{ws_status}</span>
        </div>
        <div class="tbl-wrap">
            <table>
                <thead><tr><th>Metric</th><th>Value</th><th>Explanation</th></tr></thead>
                <tbody>
                    <tr><td>WS Updates Received</td><td class="pos"><strong>{ws_updates}</strong></td><td>Real-time position changes detected</td></tr>
                    <tr><td>REST Fallbacks</td><td><strong>{rest_fallbacks}</strong></td><td>Times REST was used (WS disconnected)</td></tr>
                    <tr><td>Connection Mode</td><td><strong>{connection_mode}</strong></td><td>{connection_desc}</td></tr>
                </tbody>
            </table>
        </div>
    </div>
    
    <!-- Cache Stats -->
    <div class="section">
        <div class="section-header">
            <span class="section-title">⚡ Cache Performance</span>
            <span class="count-pill">{cache_hit_rate}% Hit Rate</span>
        </div>
        <div class="tbl-wrap">
            <table>
                <thead><tr><th>Metric</th><th>Value</th><th>Explanation</th></tr></thead>
                <tbody>
                    <tr><td>Cache Hits</td><td class="pos"><strong>{cache_hits}</strong></td><td>API calls saved (fast responses)</td></tr>
                    <tr><td>Cache Misses</td><td><strong>{cache_misses}</strong></td><td>Actual API calls made</td></tr>
                    <tr><td>Hit Rate</td><td class="pos"><strong>{cache_hit_rate}%</strong></td><td>Percentage served from cache</td></tr>
                </tbody>
            </table>
        </div>
    </div>
    
    <!-- Sell Filter Stats -->
    <div class="section">
        <div class="section-header">
            <span class="section-title">🛡️ Share-Based Sell Filter</span>
            <span class="count-pill">{min_sell_percent}% Threshold</span>
        </div>
        <div class="tbl-wrap">
            <table>
                <thead><tr><th>Metric</th><th>Value</th><th>Explanation</th></tr></thead>
                <tbody>
                    <tr><td>Cumulative Triggers</td><td class="pos"><strong>{cumulative_triggers}</strong></td><td>Small sells that accumulated to trigger a sell</td></tr>
                    <tr><td>Sells Skipped</td><td class="{skip_class}"><strong>{sells_skipped}</strong></td><td>Small sells ignored to save gas fees</td></tr>
                    <tr><td>Sells Executed</td><td class="pos"><strong>{sells_executed}</strong></td><td>Large sells that were copied</td></tr>
                    <tr><td>Skip Rate</td><td><strong>{skip_rate}%</strong></td><td>Percentage of sells filtered out</td></tr>
                </tbody>
            </table>
        </div>
    </div>
    
    <div class="stats">
        <div class="stat-card"><div class="stat-label">Balance</div><div class="stat-value">${balance:.2f}</div><div class="stat-sub">Peak ${peak:.2f}</div></div>
        <div class="stat-card"><div class="stat-label">Available</div><div class="stat-value">${available:.2f}</div><div class="stat-sub">After reserves</div></div>
        <div class="stat-card"><div class="stat-label">Compounding</div><div class="stat-value {comp_cls}">${comp_bankroll:.2f}</div><div class="stat-sub">Rate {comp_rate:.0f}%</div></div>
        <div class="stat-card"><div class="stat-label">Total PnL</div><div class="stat-value {total_pnl_cls}">{total_pnl_sign}${total_pnl_abs}</div><div class="stat-sub">Realised + Unrealised</div></div>
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

def build_dashboard(bot) -> dict:
    def _sign(v): return "+" if v > 0 else ("-" if v < 0 else "")
    def _cls(v): return "pos" if v > 0 else ("neg" if v < 0 else "neu")

    bankroll = bot.balance.cached_balance or 0.0
    available = bot._available_balance()
    drawdown = ((peak_bankroll - bankroll) / peak_bankroll * 100) if peak_bankroll > 0 else 0.0
    is_paused = bool(bot_paused_until and datetime.now() < bot_paused_until)

    status_label = "Paused" if is_paused else "Running"
    status_badge = "badge-paused" if is_paused else "badge-live"
    mode_label = "Dry Run" if bot.dry_run else "Live"
    mode_badge = "badge-dry" if bot.dry_run else "badge-live"

    cache_stats = orderbook_cache.get_stats()
    filter_metrics = sell_metrics.get_metrics()
    skip_rate = filter_metrics.get('skip_rate_percent', 0)
    skip_class = "metric-good" if skip_rate > 20 else "metric-bad"
    
    ws_status = "CONNECTED" if bot.ws_connected else "FALLBACK"
    connection_mode = "WebSocket Real-Time" if bot.ws_connected else "REST Polling"
    connection_desc = "Instant updates (<1s)" if bot.ws_connected else f"Polling every {POLL_INTERVAL}s"

    unrealised = 0.0
    pos_rows = ""
    for p in bot.positions.values():
        mid = p.current_price if p.current_price > 0 else p.entry_price
        unreal = (mid - p.entry_price) * p.shares
        unrealised += unreal
        outcome_cls = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        pnl_cls = _cls(unreal)
        pnl_fmt = ".4f" if abs(unreal) < 0.005 else ".2f"
        pnl_str = f"{_sign(unreal)}${abs(unreal):{pnl_fmt}}"
        cur_str = f"{mid:.3f}" if p.current_price > 0 else "—"
        cum_str = f"<span style='font-size:0.65rem;color:#475569;'>Cum: {p.cumulative_sold_percent:.1f}%</span>" if p.cumulative_sold_percent > 0 else ""
        pos_rows += f"<tr><td><span class='source-tag'>{p.source_name}</span>{cum_str}</td><td class='market-name'>{p.question[:60]}</td><td><span class='outcome-pill {outcome_cls}'>{p.outcome}</span></td><td>${p.size_usd:.2f}<br><span style='font-size:0.70rem;color:#475569;'>{p.shares:.4f} shares</span></td><td class='price-mono'>{p.entry_price:.3f}</td><td class='price-mono'>{cur_str}</td><td class='pnl-cell {pnl_cls}'>{pnl_str}</td></tr>"

    if pos_rows:
        positions_block = f'<div class="tbl-wrap"><table><thead><tr><th>Source</th><th>Market</th><th>Side</th><th>Size</th><th>Entry</th><th>Current</th><th>Unreal PnL</th></tr></thead><tbody>{pos_rows}</tbody></table></div>'
    else:
        positions_block = '<div class="empty"><div class="empty-icon">📭</div>No open positions</div>'

    closed_list = getattr(bot, "closed_positions", [])
    realised = sum(p.pnl for p in closed_list)
    closed_rows = ""
    for p in reversed(closed_list):
        outcome_cls = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        pnl_cls = _cls(p.pnl)
        pnl_str = f"{_sign(p.pnl)}${abs(p.pnl):.2f}"
        closed_rows += f"<tr><td><span class='source-tag'>{p.source_name}</span></td><td class='market-name'>{p.question[:60]}</td><td><span class='outcome-pill {outcome_cls}'>{p.outcome}</span></td><td class='price-mono'>{p.entry_price:.3f}</td><td class='price-mono'>{p.exit_price:.3f}</td><td class='pnl-cell {pnl_cls}'>{pnl_str}</td></tr>"

    if closed_rows:
        closed_block = f'<div class="tbl-wrap"><table><thead><tr><th>Source</th><th>Market</th><th>Side</th><th>Entry</th><th>Exit</th><th>Realised PnL</th></tr></thead><tbody>{closed_rows}</tbody></table></div>'
    else:
        closed_block = '<div class="empty"><div class="empty-icon">📋</div>No closed trades yet</div>'

    total_pnl = realised + unrealised
    comp_delta = compounding_bankroll - (bot.balance.peak_balance or INITIAL_BANKROLL)

    def _fmt(v): return f"{abs(v):.4f}" if abs(v) < 0.005 else f"{abs(v):.2f}"

    return {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode_label": mode_label, "mode_badge": mode_badge,
        "status_label": status_label, "status_badge": status_badge,
        "balance": bankroll, "available": available, "peak": peak_bankroll,
        "drawdown": drawdown, "dd_cls": "neg" if drawdown > 10 else ("neu" if drawdown > 5 else "pos"), "max_dd": MAX_DRAWDOWN * 100,
        "comp_bankroll": compounding_bankroll, "comp_cls": _cls(comp_delta), "comp_rate": COMPOUNDING_RATE * 100,
        "total_pnl_cls": _cls(total_pnl), "total_pnl_sign": _sign(total_pnl), "total_pnl_abs": _fmt(total_pnl),
        "unreal_cls": _cls(unrealised), "unreal_sign": _sign(unrealised), "unreal_abs": _fmt(unrealised),
        "real_cls": _cls(realised), "real_sign": _sign(realised), "real_abs": _fmt(realised),
        "open_count": len(bot.positions), "closed_count": len(closed_list),
        "positions_block": positions_block, "closed_block": closed_block,
        "cache_hits": cache_stats['hits'], "cache_misses": cache_stats['misses'], "cache_hit_rate": cache_stats['hit_rate_percent'],
        "min_sell_percent": MIN_SELL_PERCENT, "sells_skipped": filter_metrics.get('sells_skipped', 0),
        "sells_executed": filter_metrics.get('sells_executed', 0), "skip_rate": skip_rate, "skip_class": skip_class,
        "skipped_shares": filter_metrics.get('skipped_shares', 0), "cumulative_triggers": filter_metrics.get('cumulative_triggers', 0),
        "ws_updates": filter_metrics.get('ws_updates_received', 0), "rest_fallbacks": filter_metrics.get('rest_fallbacks_used', 0),
        "ws_status": ws_status, "connection_mode": connection_mode, "connection_desc": connection_desc,
    }

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200); self.send_header("Content-Type", "text/html"); self.end_headers()
            try:
                data = build_dashboard(_bot_ref)
                self.wfile.write(HTML_TEMPLATE.format(**data).encode())
            except Exception as e: self.wfile.write(b"<h1>Dashboard loading...</h1>")
        elif self.path == "/metrics":
            self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers()
            metrics = sell_metrics.get_metrics()
            metrics['cache'] = orderbook_cache.get_stats()
            if _bot_ref: metrics.update({'balance': _bot_ref.balance.cached_balance or 0, 'open_positions': len(_bot_ref.positions), 'ws_connected': _bot_ref.ws_connected})
            self.wfile.write(json.dumps(metrics, indent=2).encode())
        elif self.path == "/health":
            self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers()
            health = {"status": "healthy", "timestamp": datetime.now().isoformat(), "ws_connected": _bot_ref.ws_connected if _bot_ref else False, "poll_interval": POLL_INTERVAL}
            self.wfile.write(json.dumps(health).encode())
        else: self.send_response(200); self.end_headers(); self.wfile.write(b"OK")
    def log_message(self, format, *args): pass

_bot_ref = None
def run_health_server(): HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler).serve_forever()

# ==================== SEEN TRADES STORE ====================
class SeenTradesStore:
    def __init__(self, filepath: str, db_url: str = ""):
        self.filepath, self.db_url = filepath, db_url
        self._seen: Set[str] = set(); self._conn = None
        if db_url and PSYCOPG2_AVAILABLE: self._init_postgres()
        else: self._load_file()
        logger.info(f"SeenTradesStore ready | backend={self.backend} | {len(self._seen)} keys")
    def _init_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require"); self._conn.autocommit = True
            with self._conn.cursor() as cur: cur.execute("CREATE TABLE IF NOT EXISTS seen_trades (pos_key TEXT PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW())")
            self._seen = self._load_postgres(); self.backend = "postgres"
        except Exception as e: logger.error(f"Postgres init failed: {e}"); self._conn = None; self._load_file()
    def _load_postgres(self) -> Set[str]:
        try:
            with self._conn.cursor() as cur: cur.execute("SELECT pos_key FROM seen_trades"); return {row[0] for row in cur.fetchall()}
        except: return set()
    def _save_postgres(self, pos_key: str):
        try:
            with self._conn.cursor() as cur: cur.execute("INSERT INTO seen_trades (pos_key) VALUES (%s) ON CONFLICT DO NOTHING", (pos_key,))
        except: self._reconnect_postgres()
    def _save_postgres_many(self, keys):
        if not keys: return
        try:
            with self._conn.cursor() as cur: psycopg2.extras.execute_values(cur, "INSERT INTO seen_trades (pos_key) VALUES %s ON CONFLICT DO NOTHING", [(k,) for k in keys])
        except: self._reconnect_postgres()
    def _reconnect_postgres(self):
        try: self._conn = psycopg2.connect(self.db_url, sslmode="require"); self._conn.autocommit = True
        except: pass
    def _load_file(self):
        try:
            with open(self.filepath, "r") as f: data = json.load(f); self._seen = set(data) if isinstance(data, list) else set()
        except: self._seen = set()
        self.backend = "local-file"
    def _save_file(self):
        try:
            with open(self.filepath, "w") as f: json.dump(sorted(self._seen), f)
        except: pass
    def is_seen(self, pos_key: str) -> bool: return pos_key in self._seen
    def mark_seen(self, pos_key: str):
        if pos_key in self._seen: return
        self._seen.add(pos_key)
        if self._conn: self._save_postgres(pos_key)
        else: self._save_file()
    def snapshot_existing(self, pos_keys):
        new_keys = [k for k in pos_keys if k not in self._seen]
        if not new_keys: return
        for k in new_keys: self._seen.add(k)
        if self._conn: self._save_postgres_many(new_keys)
        else: self._save_file()
        logger.info(f"Snapshot: marked {len(new_keys)} existing trades")

# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    POLYGON_RPCS = ["https://polygon-bor-rpc.publicnode.com", "https://polygon.llamarpc.com", "https://polygon.drpc.org"]
    def __init__(self): self.cached_balance: Optional[float] = None; self.last_update = 0; self.peak_balance = 0.0
    def _fetch_balance(self) -> float:
        if not YOUR_WALLET: return 0.0
        padded = YOUR_WALLET.lower().replace("0x", "").zfill(64)
        payload = {"jsonrpc": "2.0", "method": "eth_call", "params": [{"to": PUSD_CONTRACT_ADDRESS, "data": "0x70a08231" + padded}, "latest"], "id": 1}
        for rpc in self.POLYGON_RPCS:
            try:
                resp = requests.post(rpc, json=payload, timeout=8)
                if resp.status_code == 200:
                    result = resp.json().get("result", "0x0")
                    if result and result not in ("0x", "0x0"): return int(result, 16) / 1_000_000
            except: continue
        return 0.0
    def get_balance(self, force=False) -> Optional[float]:
        if force or self.cached_balance is None or (time.time() - self.last_update > 30):
            real = self._fetch_balance()
            if real > 0: self.cached_balance = real; self.last_update = time.time()
            if real > self.peak_balance: self.peak_balance = real
        return self.cached_balance
    def fetch_with_retry(self, retries=5, delay=10) -> float:
        for attempt in range(1, retries + 1):
            val = self._fetch_balance()
            if val > 0: self.cached_balance = val; self.peak_balance = val; return val
            time.sleep(delay)
        raise RuntimeError("Could not fetch balance")
    def check_drawdown(self) -> Tuple[bool, float]:
        current = self.get_balance()
        if current is None or self.peak_balance == 0: return False, 0.0
        dd = (self.peak_balance - current) / self.peak_balance
        return dd >= MAX_DRAWDOWN, dd

# ==================== POLYMARKET EXECUTOR ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run; self.client = None
        if not dry_run and CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(api_key=POLY_API_KEY, api_secret=POLY_SECRET, api_passphrase=POLY_PASSPHRASE)
                self.client = ClobClient(host="https://clob.polymarket.com", chain_id=137, key=YOUR_PRIVATE_KEY, creds=creds)
                logger.info("ClobClient V2 initialised — LIVE mode")
            except Exception as e: logger.error(f"ClobClient init failed: {e}")
    def place_limit_buy(self, token_id: str, amount_usd: float, limit_price: float) -> Tuple[bool, str, float]:
        shares = round(amount_usd / limit_price, 4)
        if self.dry_run or self.client is None: return True, "dry-run-limit-buy", limit_price
        for attempt in range(MAX_RETRIES):
            try:
                result = self.client.create_and_post_order(order_args=OrderArgs(token_id=token_id, price=limit_price, size=shares, side=Side.BUY), options=PartialCreateOrderOptions(tick_size="0.01"), order_type=OrderType.GTC)
                return True, result.get("orderID", result.get("id", "unknown")), limit_price
            except: time.sleep(RETRY_DELAY)
        return False, "", limit_price
    def cancel_order(self, order_id: str) -> bool:
        if self.dry_run or self.client is None: return True
        try: self.client.cancel(order_id); return True
        except: return False
    def is_order_filled(self, order_id: str) -> Optional[bool]:
        if self.dry_run or self.client is None: return True
        try:
            order = self.client.get_order(order_id)
            return order.get("status", "").lower() in ("matched", "filled")
        except: return None
    def place_sell_with_partial_fill_handling(self, token_id: str, total_shares: float, min_price: float = 0.0, max_attempts: int = 3) -> Tuple[bool, float, List[float]]:
        if self.dry_run or self.client is None: return True, total_shares, [0.0]
        remaining_shares, executed_shares, prices = total_shares, 0.0, []
        for attempt in range(max_attempts):
            if remaining_shares <= 0.01: break
            try:
                kwargs = dict(order_args=MarketOrderArgs(token_id=token_id, amount=remaining_shares, side=Side.SELL), options=PartialCreateOrderOptions(tick_size="0.01"), order_type=OrderType.IOC)
                if min_price > 0:
                    try: kwargs["order_args"] = MarketOrderArgs(token_id=token_id, amount=remaining_shares, side=Side.SELL, min_price=round(min_price, 4))
                    except: pass
                result = self.client.create_and_post_market_order(**kwargs)
                filled_size = float(result.get("filledSize", result.get("size", remaining_shares)))
                avg_price = float(result.get("averagePrice", min_price))
                if filled_size > 0:
                    executed_shares += filled_size; remaining_shares -= filled_size; prices.append(avg_price)
                    if remaining_shares <= 0.01: return True, executed_shares, prices
                    time.sleep(2)
                else: time.sleep(RETRY_DELAY)
            except: time.sleep(RETRY_DELAY)
        return executed_shares > 0, executed_shares, prices

# ==================== WEBSOCKET MANAGER ====================
class WebSocketManager:
    def __init__(self, bot):
        self.bot = bot
        self.ws = None
        self.connected = False
        self.stop_flag = False
        self.reconnect_delay = WEBSOCKET_RECONNECT_DELAY
        self.heartbeat_interval = 10
        self._thread = None
    
    def start(self):
        if not WEBSOCKET_ENABLED:
            logger.info("WebSocket disabled, using REST only")
            return
        self._thread = threading.Thread(target=self._run_websocket, daemon=True)
        self._thread.start()
    
    def stop(self):
        self.stop_flag = True
        if self.ws:
            self.ws.close()
    
    def _run_websocket(self):
        while not self.stop_flag:
            try:
                self._connect()
                self._subscribe()
                self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
            if not self.stop_flag:
                logger.info(f"Reconnecting in {self.reconnect_delay}s...")
                time.sleep(self.reconnect_delay)
    
    def _connect(self):
        self.ws = websocket.WebSocketApp(
            WEBSOCKET_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
    
    def _on_open(self, ws):
        logger.info("✅ WebSocket connected")
        self.connected = True
        self.bot.ws_connected = True
        
        # Start heartbeat thread
        def send_heartbeat():
            while self.connected and not self.stop_flag:
                time.sleep(self.heartbeat_interval)
                if self.ws and self.ws.sock and self.ws.sock.connected:
                    try:
                        self.ws.send(json.dumps({"type": "ping"}))
                    except: pass
        threading.Thread(target=send_heartbeat, daemon=True).start()
    
    def _subscribe(self):
        if not self.ws: return
        
        # Generate authentication for user channel
        timestamp = str(int(time.time()))
        method = "GET"
        request_path = ""
        message = timestamp + method + request_path
        signature = hmac.new(POLY_SECRET.encode(), message.encode(), hashlib.sha256).digest()
        signature_b64 = base64.b64encode(signature).decode()
        
        # Subscribe to user channel for each wallet
        for wallet_addr in WALLETS.keys():
            auth_msg = {
                "type": "subscribe",
                "channel": "user",
                "apiKey": POLY_API_KEY,
                "timestamp": timestamp,
                "signature": signature_b64,
                "passphrase": POLY_PASSPHRASE,
                "address": wallet_addr
            }
            self.ws.send(json.dumps(auth_msg))
            logger.info(f"Subscribed to user channel for {wallet_addr[:10]}...")
        
        # Subscribe to market channel for orderbook updates
        market_msg = {"type": "subscribe", "channel": "market", "assets_ids": []}
        self.ws.send(json.dumps(market_msg))
    
    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            
            # Handle heartbeat pong
            if data.get("type") == "pong":
                return
            
            # Handle position updates
            if data.get("channel") == "user" and data.get("event_type") == "position_change":
                self.bot.handle_ws_position_update(data)
                sell_metrics.record_ws_update()
            
            # Handle orderbook updates (for real-time price tracking)
            if data.get("channel") == "market" and data.get("event_type") == "book":
                self.bot.handle_ws_orderbook_update(data)
                
        except Exception as e:
            logger.error(f"WebSocket message error: {e}")
    
    def _on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")
        self.connected = False
        self.bot.ws_connected = False
    
    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.connected = False
        self.bot.ws_connected = False

# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.balance = RobustBalanceManager()
        self.positions: Dict[str, Position] = {}
        self.pending: Dict[str, PendingLimitBuy] = {}
        self.executor = PolymarketExecutor(dry_run)
        self.seen = SeenTradesStore(SEEN_TRADES_FILE, DATABASE_URL)
        self._first_scan_done: Set[str] = set()
        self.closed_positions: list = []
        self.ws_connected = False
        self.last_rest_scan = 0
        
        # Initialize WebSocket
        self.ws_manager = WebSocketManager(self)
        self.ws_manager.start()
        
        logger.info(f"CopyTrader started | mode={'DRY RUN' if dry_run else 'LIVE'}")
        logger.info(f"⚡ WebSocket: {'ENABLED' if WEBSOCKET_ENABLED else 'DISABLED'}")
        logger.info(f"📊 REST Fallback: {POLL_INTERVAL}s")
    
    def _reserved_capital(self) -> float:
        return sum(p.size_usd for p in self.positions.values()) + sum(p.size_usd for p in self.pending.values())
    
    def _available_balance(self) -> float:
        bal = self.balance.cached_balance or 0.0
        return max(0.0, bal - self._reserved_capital())
    
    def _can_afford(self, amount_usd: float) -> bool:
        available = self._available_balance()
        can = available >= amount_usd * 1.02
        if not can: logger.warning(f"Affordability failed: need ${amount_usd:.2f}, available=${available:.2f}")
        return can
    
    # ==================== CACHED API METHODS ====================
    def get_orderbook_prices(self, token_id: str) -> Tuple[float, float]:
        cached = orderbook_cache.get(token_id)
        if cached: return cached
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8)
                if r.status_code == 200:
                    data = r.json()
                    bids, asks = data.get("bids", []), data.get("asks", [])
                    best_bid = float(bids[0]["price"]) if bids else 0.0
                    best_ask = float(asks[0]["price"]) if asks else 0.0
                    mid = (best_bid + best_ask) / 2 if best_bid and best_ask else best_bid or best_ask
                    result = (mid, best_ask)
                    orderbook_cache.set(token_id, result)
                    return result
            except: time.sleep(RETRY_DELAY)
        return 0.0, 0.0
    
    def _get_best_bid(self, token_id: str) -> float:
        cached = bid_cache.get(token_id)
        if cached: return cached
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8)
                if r.status_code == 200:
                    bids = r.json().get("bids", [])
                    best_bid = float(bids[0]["price"]) if bids else 0.0
                    if best_bid > 0: bid_cache.set(token_id, best_bid)
                    return best_bid
            except: time.sleep(RETRY_DELAY)
        return 0.0
    
    def get_risk_percent(self, price: float, config: dict) -> float:
        if config.get("risk_type") == "fixed": return config.get("fixed_risk", 0.025)
        return 0.03 if price >= 0.70 else (0.01 if price >= 0.30 else 0.006)
    
    def check_drawdown(self) -> bool:
        global peak_bankroll, bot_paused_until
        current = self.balance.get_balance()
        if current > peak_bankroll: peak_bankroll = current
        dd = (peak_bankroll - current) / peak_bankroll if peak_bankroll > 0 else 0
        if dd >= MAX_DRAWDOWN:
            if bot_paused_until is None or datetime.now() > bot_paused_until:
                bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
                logger.warning(f"DRAWDOWN TRIGGERED ({dd*100:.1f}%) — paused {PAUSE_HOURS}h")
            return True
        return False
    
    def _get_positions_rest(self, wallet_addr: str) -> list | None:
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50", timeout=12)
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 30))
                    time.sleep(retry_after)
                    continue
                if resp.status_code == 200: return resp.json()
            except: time.sleep(RETRY_DELAY)
        return None
    
    # ==================== WEBSOCKET HANDLERS ====================
    def handle_ws_position_update(self, data):
        """Handle real-time position updates from WebSocket"""
        wallet_addr = data.get("address")
        token_id = data.get("asset")
        new_shares = float(data.get("size", 0))
        
        if not wallet_addr or not token_id:
            return
        
        pos_key = f"{wallet_addr}_{token_id}"
        
        # Update our tracked source shares
        for position in self.positions.values():
            if position.source_wallet == wallet_addr and position.token_id == token_id:
                old_shares = position.source_shares
                if new_shares != old_shares:
                    logger.info(f"📡 WS: {position.source_name} position changed: {old_shares:.4f} → {new_shares:.4f} shares")
                    # Process the sell immediately
                    self._process_position_change(position, pos_key, new_shares)
                break
    
    def handle_ws_orderbook_update(self, data):
        """Handle real-time orderbook updates"""
        token_id = data.get("asset_id")
        if not token_id:
            return
        
        # Update cache with fresh orderbook data
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if bids and asks:
            best_bid = float(bids[0][0]) if bids else 0.0
            best_ask = float(asks[0][0]) if asks else 0.0
            mid = (best_bid + best_ask) / 2 if best_bid and best_ask else best_bid or best_ask
            orderbook_cache.set(token_id, (mid, best_ask))
            if best_bid > 0:
                bid_cache.set(token_id, best_bid)
    
    def _process_position_change(self, position: Position, pos_key: str, new_source_shares: float):
        """Process a position change detected via WebSocket"""
        
        # Full exit
        if new_source_shares <= 0:
            logger.info(f"🚨 WS FULL EXIT: {position.question[:40]} — selling all {position.shares:.4f} shares")
            self._execute_sell(position, pos_key, position.shares, position.source_name, full_exit=True)
            return
        
        # Partial exit
        if new_source_shares < position.source_shares_at_open:
            current_sell_percent = ((position.source_shares_at_open - new_source_shares) / position.source_shares_at_open) * 100
            position.cumulative_sold_percent += current_sell_percent
            
            total_sold_ratio = min(position.cumulative_sold_percent / 100, 0.99)
            target_shares = position.shares_at_open * (1 - total_sold_ratio)
            shares_to_sell = position.shares - target_shares
            
            if position.cumulative_sold_percent >= MIN_SELL_PERCENT and shares_to_sell > 0.01:
                logger.info(f"✅ WS CUMULATIVE: {position.cumulative_sold_percent:.1f}% reached, selling {shares_to_sell:.4f} shares")
                sell_metrics.record_decision(True, shares_to_sell, position.cumulative_sold_percent, True)
                self._execute_sell(position, pos_key, shares_to_sell, position.source_name, full_exit=False, current_source_shares=new_source_shares)
                position.cumulative_sold_percent = 0.0
                position.source_shares_at_open = new_source_shares
                position.shares_at_open = position.shares
            else:
                logger.info(f"📡 WS accumulating: {position.cumulative_sold_percent:.1f}% total (need {MIN_SELL_PERCENT}%)")
                position.source_shares_at_open = new_source_shares
                position.shares_at_open = position.shares
        
        # Update current shares
        position.source_shares = new_source_shares
    
    # ==================== REST FALLBACK SCAN ====================
    async def rest_fallback_scan(self):
        """Fallback REST scan when WebSocket is disconnected"""
        if self.ws_connected:
            return  # WebSocket is working, skip REST
        
        sell_metrics.record_rest_fallback()
        logger.debug("WebSocket disconnected, using REST fallback")
        
        for wallet_addr, config in WALLETS.items():
            raw = self._get_positions_rest(wallet_addr)
            if not raw:
                continue
            
            source_shares_map = {}
            for pos in raw:
                tid = pos.get("asset", "")
                shares = float(pos.get("size", pos.get("shares", 0)))
                if tid and shares > 0:
                    source_shares_map[tid] = shares
            
            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr:
                    continue
                if position.status != "open":
                    continue
                
                current_source_shares = source_shares_map.get(position.token_id, 0.0)
                
                if current_source_shares <= 0:
                    self._execute_sell(position, pos_key, position.shares, config["name"], full_exit=True)
                elif current_source_shares < position.source_shares_at_open:
                    current_sell_percent = ((position.source_shares_at_open - current_source_shares) / position.source_shares_at_open) * 100
                    position.cumulative_sold_percent += current_sell_percent
                    
                    total_sold_ratio = min(position.cumulative_sold_percent / 100, 0.99)
                    target_shares = position.shares_at_open * (1 - total_sold_ratio)
                    shares_to_sell = position.shares - target_shares
                    
                    if position.cumulative_sold_percent >= MIN_SELL_PERCENT and shares_to_sell > 0.01:
                        logger.info(f"✅ REST FALLBACK: Cumulative {position.cumulative_sold_percent:.1f}%, selling {shares_to_sell:.4f} shares")
                        self._execute_sell(position, pos_key, shares_to_sell, config["name"], full_exit=False, current_source_shares=current_source_shares)
                        position.cumulative_sold_percent = 0.0
                        position.source_shares_at_open = current_source_shares
                        position.shares_at_open = position.shares
                    else:
                        position.source_shares_at_open = current_source_shares
                        position.shares_at_open = position.shares
                    
                    position.source_shares = current_source_shares
    
    # ==================== SELL EXECUTION ====================
    def _execute_sell(self, position: Position, pos_key: str, shares_to_sell: float, name: str, full_exit: bool, current_source_shares: float = 0.0):
        global compounding_bankroll
        
        if shares_to_sell <= 0:
            return
        
        if self.dry_run:
            exit_price = position.current_price if position.current_price > 0 else position.entry_price
            pnl = (exit_price - position.entry_price) * shares_to_sell
            success, executed_shares = True, shares_to_sell
        else:
            best_bid = self._get_best_bid(position.token_id)
            min_price = round(best_bid * (1 - MAX_SLIPPAGE), 4) if best_bid > 0 else 0.0
            
            pending_costs_before = {pk: p.size_usd for pk, p in self.pending.items()}
            
            with _trade_lock:
                balance_before = self.balance.get_balance(force=True) or 0.0
                success, executed_shares, prices = self.executor.place_sell_with_partial_fill_handling(position.token_id, shares_to_sell, min_price)
                
                if success and executed_shares > 0:
                    time.sleep(SELL_SETTLE_WAIT)
                    balance_after = self.balance.get_balance(force=True) or 0.0
                    
                    contamination = sum(cost for pk, cost in pending_costs_before.items() if pk not in self.pending and pk in self.positions)
                    raw_diff = balance_after - balance_before
                    pnl = (raw_diff + contamination) * (executed_shares / shares_to_sell)
                    exit_price = best_bid if best_bid > 0 else position.current_price
                else:
                    pnl, exit_price = 0.0, 0.0
        
        if not success or executed_shares <= 0:
            logger.error(f"SELL failed: {position.question[:40]}")
            return
        
        if full_exit or executed_shares >= position.shares - 0.001:
            position.status = "closed"
            position.exit_price = exit_price
            position.pnl = pnl
            if pnl > 0:
                compounding_bankroll += pnl * COMPOUNDING_RATE
            logger.info(f"{'FULL' if full_exit else 'PARTIAL'} SELL | {position.question[:40]} | pnl=${pnl:.4f}")
            self.closed_positions.append(position)
            del self.positions[pos_key]
        else:
            position.shares -= executed_shares
            position.size_usd = position.shares * position.entry_price
            position.source_shares = current_source_shares
            if pnl > 0:
                compounding_bankroll += pnl * COMPOUNDING_RATE
    
    async def run(self):
        logger.info(f"Bot running | Poll: {POLL_INTERVAL}s | WS: {'Enabled' if WEBSOCKET_ENABLED else 'Disabled'} | Min sell: {MIN_SELL_PERCENT}%")
        
        last_heartbeat = time.time()
        last_rest_scan = 0
        
        while True:
            try:
                now = time.time()
                
                # Run REST fallback every POLL_INTERVAL seconds (only when WS disconnected)
                if now - last_rest_scan >= POLL_INTERVAL:
                    await self.rest_fallback_scan()
                    last_rest_scan = now
                
                if now - last_heartbeat >= 300:
                    status = "PAUSED" if bot_paused_until and datetime.now() < bot_paused_until else "ACTIVE"
                    ws_status = "CONNECTED" if self.ws_connected else "DISCONNECTED"
                    logger.info(f"Heartbeat | {status} | WS:{ws_status} | balance=${self.balance.cached_balance or 0:.2f} | open={len(self.positions)}")
                    last_heartbeat = now
                
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Main loop error: {e}")
                await asyncio.sleep(5)

# ==================== ENTRY POINT ====================
async def main():
    global _bot_ref, compounding_bankroll, peak_bankroll
    
    threading.Thread(target=run_health_server, daemon=True).start()
    
    bot = CopyTrader(dry_run=DRY_RUN)
    _bot_ref = bot
    
    logger.info("=" * 60)
    logger.info("🤖 WEBSOCKET + REST HYBRID COPY TRADER")
    logger.info(f"📡 WebSocket: {'ENABLED (Real-time)' if WEBSOCKET_ENABLED else 'DISABLED'}")
    logger.info(f"📊 REST Fallback: Every {POLL_INTERVAL} seconds")
    logger.info(f"🛡️ Sell threshold: {MIN_SELL_PERCENT}%")
    logger.info("=" * 60)
    
    try:
        starting_balance = bot.balance.fetch_with_retry(retries=5, delay=10)
        bot.balance.peak_balance = starting_balance
        peak_bankroll = starting_balance
        compounding_bankroll = starting_balance
        logger.info(f"Balance seeded: ${starting_balance:.2f}")
    except RuntimeError as e:
        logger.error(f"Balance fetch failed: {e}")
    
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
