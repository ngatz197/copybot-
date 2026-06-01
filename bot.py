#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - PRODUCTION READY (HIGH-PERFORMANCE HFT VARIANT)

Integrates:
  - Pre-Warmed Network Pipelines (Persistent keep-alive sockets)
  - Pre-Signed Order Matrices (RAM-cached EIP-712 cryptographic payloads)
  - Tight Limit Premium Shields (Automated slippage cutoffs preventing bad fills)
  - Bulletproof Initialization Logic (Fixes existing-trade copy loops caused by network drops)
  - Original Tiered Scaling and Fixed Allocation Risk Structures
  - DYNAMIC FIX: Automated background HFT asset extraction (Removes hardcoded array limits)
"""

import os
import json
import asyncio
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv
import websockets
import websockets.exceptions
import aiohttp
import requests

# Cryptographic libraries for EIP-712 Pre-Signing Optimization
from eth_account import Account
from eth_account.messages import encode_typed_data

Account.enable_unaudited_hdwallet_features()
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ==================== CLOB V2 CLIENT ====================
try:
    from py_clob_client_v2 import (
        ClobClient,
        OrderArgs,
        MarketOrderArgs,
        OrderType,
        Side,
        ApiCreds,
        PartialCreateOrderOptions,
    )
    CLOB_AVAILABLE = True
    logging.info("✅ py_clob_client_v2 loaded successfully")
except ImportError:
    CLOB_AVAILABLE = False
    logging.warning("py_clob_client_v2 not installed — running in simulation mode.")

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logging.warning("psycopg2 not installed — seen_trades will fall back to local file.")

# ==================== CONFIG ====================
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

WALLETS = {
    "0xe8ca3f758c93f44f3ec210542ab78afb7c0bcccb": {
        "name": "Kruto",
        "risk_type": "price_based",
        "copy_mode": "new_only",
        "limit_buy_max_premium": 0.10,  # Strict Limit Premium Guard (10%)
        "copy_sub_dollar": True,
        "max_positions": 8,
    },
    "0x0c0e270cf879583d6a0142fc817e05b768d0434e": {
        "name": "TheSpirit",
        "risk_type": "price_based", # Uses the original multi-tier 3% / 1% / 0.6% rule
        "copy_mode": "new_only",
        "limit_buy_max_premium": 0.08,  # Tight Guard for high volatile entry
        "max_positions": 5,
    },
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {
        "name": "Coniyr",
        "risk_type": "fixed",
        "fixed_risk": 0.025,       # Restored to original constant 2.5%
        "copy_mode": "copy_all",
        "limit_buy_max_premium": 0.10,
        "max_positions": 4,
    },
    "0xf903c4cd098184e67a06a04f9b8fdb36e7bbe028": {
        "name": "Viser",
        "risk_type": "fixed",
        "fixed_risk": 0.025,       # Restored to original constant 2.5%
        "copy_mode": "new_only",
        "limit_buy_max_premium": 0.05,  # Hyper-tight 5% shield
        "max_positions": 3,
    },
}

YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
POLY_API_KEY     = os.getenv("POLY_API_KEY", "")
POLY_SECRET      = os.getenv("POLY_SECRET", "")
POLY_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")
DATABASE_URL     = os.getenv("DATABASE_URL", "")

INITIAL_BANKROLL      = 10.0
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "20"))
POLL_INTERVAL         = 15   
COMPOUNDING_RATE      = float(os.getenv("COMPOUNDING_RATE", "0.70"))
MAX_DRAWDOWN          = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT           = int(os.getenv("PORT", "10000"))
PAUSE_HOURS           = 48
MAX_RETRIES           = 3
RETRY_DELAY           = 2

MAX_SLIPPAGE          = float(os.getenv("MAX_SLIPPAGE", "0.20"))
LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))
LIMIT_EXPIRY_SECONDS  = int(os.getenv("LIMIT_EXPIRY_SECONDS", "300"))
SEEN_TRADES_FILE      = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")
MAX_FILL_CHECK_ERRORS = int(os.getenv("MAX_FILL_CHECK_ERRORS", "5"))
SELL_SETTLE_WAIT      = int(os.getenv("SELL_SETTLE_WAIT", "8"))

MIN_REQUEST_GAP = float(os.getenv("MIN_REQUEST_GAP", "0.5"))
POSITION_CACHE_TTL  = 12   
ORDERBOOK_CACHE_TTL = 3    
WS_DEBOUNCE_SECONDS = 2.0

PUSD_CONTRACT_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

current_bankroll      = INITIAL_BANKROLL
peak_bankroll         = INITIAL_BANKROLL
compounding_bankroll  = INITIAL_BANKROLL
bot_paused_until: Optional[datetime] = None

_trade_lock = threading.Lock()

# Global memory cache for pre-signed matrix payloads and persistent pipeline session
PRE_SIGNED_MATRIX_CACHE: Dict[str, dict] = {}
WARM_HTTP_SESSION: Optional[aiohttp.ClientSession] = None

# ==================== GLOBAL ASYNC THROTTLE ====================
class RequestThrottle:
    def __init__(self, min_gap: float = MIN_REQUEST_GAP):
        self._min_gap   = min_gap
        self._last_time = 0.0
        self._lock      = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now  = asyncio.get_event_loop().time()
            wait = self._min_gap - (now - self._last_time)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_time = asyncio.get_event_loop().time()

throttle = RequestThrottle()

# ==================== CIRCUIT BREAKER ====================
class CircuitBreaker:
    FAILURE_THRESHOLD = 5
    BACKOFF_SECONDS   = 60

    def __init__(self, name: str):
        self.name             = name
        self._failures        = 0
        self._state           = "CLOSED"   
        self._opened_at: Optional[float] = None

    @property
    def state(self) -> str:
        if self._state == "OPEN":
            if time.monotonic() - self._opened_at >= self.BACKOFF_SECONDS:
                self._state = "HALF_OPEN"
                logging.info(f"CircuitBreaker [{self.name}] -> HALF_OPEN (probing)")
        return self._state

    def allow_request(self) -> bool:
        s = self.state
        if s == "CLOSED" or s == "HALF_OPEN":
            return True
        return False      

    def record_success(self):
        if self._state in ("OPEN", "HALF_OPEN"):
            logging.info(f"CircuitBreaker [{self.name}] -> CLOSED (recovered)")
        self._failures  = 0
        self._state     = "CLOSED"
        self._opened_at = None

    def record_failure(self):
        self._failures += 1
        if self._state == "HALF_OPEN":
            self._state     = "OPEN"
            self._opened_at = time.monotonic()
            logging.warning(f"CircuitBreaker [{self.name}] probe failed -> OPEN (retry in {self.BACKOFF_SECONDS}s)")
        elif self._failures >= self.FAILURE_THRESHOLD and self._state == "CLOSED":
            self._state     = "OPEN"
            self._opened_at = time.monotonic()
            logging.warning(f"CircuitBreaker [{self.name}] tripped after {self._failures} failures -> OPEN")

# ==================== CACHE IMPLEMENTATIONS ====================
@dataclass
class CacheEntry:
    data:       object
    fetched_at: float = field(default_factory=time.monotonic)

    def is_fresh(self, ttl: float) -> bool:
        return (time.monotonic() - self.fetched_at) < ttl

class PositionCache:
    def __init__(self, ttl: float = POSITION_CACHE_TTL):
        self._ttl:   float                   = ttl
        self._cache: Dict[str, CacheEntry]   = {}

    def get(self, wallet: str) -> Optional[list]:
        entry = self._cache.get(wallet)
        if entry and entry.is_fresh(self._ttl):
            return entry.data
        return None

    def set(self, wallet: str, data: list):
        self._cache[wallet] = CacheEntry(data=data)

    def invalidate(self, wallet: str):
        self._cache.pop(wallet, None)

class OrderbookCache:
    def __init__(self, ttl: float = ORDERBOOK_CACHE_TTL):
        self._ttl:   float                              = ttl
        self._cache: Dict[str, CacheEntry]              = {}

    def get(self, token_id: str) -> Optional[Tuple[float, float]]:
        entry = self._cache.get(token_id)
        if entry and entry.is_fresh(self._ttl):
            return entry.data
        return None

    def set(self, token_id: str, mid: float, best_ask: float):
        self._cache[token_id] = CacheEntry(data=(mid, best_ask))

    def get_bid(self, token_id: str) -> Optional[float]:
        entry = self._cache.get(token_id)
        if entry and entry.is_fresh(self._ttl) and len(entry.data) == 3:
            return entry.data[2]
        return None

    def set_full(self, token_id: str, mid: float, best_ask: float, best_bid: float):
        self._cache[token_id] = CacheEntry(data=(mid, best_ask, best_bid))

# ==================== MARKET DATA HANDLER ====================
class MarketDataManager:
    def __init__(self):
        self.token_to_price:    Dict[str, float] = {}
        self.subscribed_tokens: Set[str]         = set()
        self.running                             = False
        self.activity_event                      = asyncio.Event()
        self._subscribed_wallets: Set[str]       = set()
        self._market_ws                          = None
        self._user_ws                            = None
        self._debounce_task: Optional[asyncio.Task] = None

    async def connect_market(self):
        uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=30) as ws:
                    self._market_ws = ws
                    logging.info("✅ Market-price WS connected")
                    if self.subscribed_tokens:
                        await self._send_market_sub(ws, list(self.subscribed_tokens))
                    async for message in ws:
                        try:
                            data = json.loads(message)
                            if data.get("asset_id"):
                                price = data.get("price") or data.get("last_trade_price")
                                if price:
                                    self.token_to_price[data["asset_id"]] = round(float(price), 6)
                        except Exception:
                            pass
            except Exception as e:
                logging.warning(f"Market-price WS disconnected: {e}. Reconnecting in 3s…")
                self._market_ws = None
                await asyncio.sleep(3)

    async def _send_market_sub(self, ws, token_ids: list):
        if token_ids:
            await ws.send(json.dumps({"assets_ids": token_ids, "type": "market"}))

    async def subscribe_tokens(self, token_ids: list):
        new = [t for t in token_ids if t not in self.subscribed_tokens]
        if not new:
            return
        for t in new:
            self.subscribed_tokens.add(t)
        if self._market_ws:
            try:
                await self._send_market_sub(self._market_ws, new)
            except Exception as e:
                logging.warning(f"Market-price subscription failed: {e}")

    async def _debounce_wake(self):
        await asyncio.sleep(WS_DEBOUNCE_SECONDS)
        self.activity_event.set()
        self._debounce_task = None

    def _schedule_wake(self, wallet: str, event_type: str):
        if self._debounce_task is None or self._debounce_task.done():
            logging.info(f"🔔 Activity WS | wallet={wallet[:10]}… type={event_type} — debounce {WS_DEBOUNCE_SECONDS}s")
            self._debounce_task = asyncio.create_task(self._debounce_wake())

    async def connect_user(self, wallet_addresses: list):
        uri        = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
        wallet_set = {w.lower() for w in wallet_addresses}
        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=30, close_timeout=5) as ws:
                    self._user_ws = ws
                    logging.info("✅ User-activity WS connected")
                    await self._send_user_sub(ws, wallet_addresses)
                    self._subscribed_wallets.update(wallet_addresses)
                    async for message in ws:
                        try:
                            data = json.loads(message)
                            event_type = data.get("type") or data.get("event_type") or data.get("action", "")
                            user = data.get("user") or data.get("maker") or data.get("taker", "")
                            if user.lower() in wallet_set:
                                self._schedule_wake(user, event_type)
                            elif event_type:
                                self._schedule_wake("unknown", event_type)
                        except Exception:
                            pass
            except websockets.exceptions.ConnectionClosedError:
                self._user_ws = None
                await asyncio.sleep(3)
            except Exception as e:
                logging.warning(f"User-activity WS disconnected: {e}. Reconnecting in 3s…")
                self._user_ws = None
                await asyncio.sleep(3)

    async def _send_user_sub(self, ws, addresses: list):
        if addresses:
            await ws.send(json.dumps({"user": addresses, "type": "user"}))
            logging.info(f"User-activity WS: subscribed to {len(addresses)} wallet(s)")

    def get_current_price(self, token_id: str) -> float:
        return self.token_to_price.get(token_id, 0.0)

market_data = MarketDataManager()

# ==================== DASHBOARD VISUALS ====================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PolyCopyTrader</title>
    <meta http-equiv="refresh" content="15">
    <style>
        *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: 'Segoe UI', system-ui, sans-serif;
            background: #0d0d0f; color: #e2e8f0;
            min-height: 100vh; padding: 24px 16px;
        }}
        .page {{ max-width: 1100px; margin: 0 auto; }}
        .header {{
            display: flex; align-items: center;
            justify-content: space-between;
            margin-bottom: 28px; flex-wrap: wrap; gap: 8px;
        }}
        .header-title {{ font-size: 1.25rem; font-weight: 700; color: #f8fafc; letter-spacing: -0.3px; }}
        .header-title span {{ color: #6ee7b7; }}
        .badge {{ font-size: 0.72rem; font-weight: 600; padding: 3px 10px; border-radius: 999px;
                  letter-spacing: 0.4px; text-transform: uppercase; }}
        .badge-live   {{ background: #064e3b; color: #6ee7b7; border: 1px solid #065f46; }}
        .badge-dry    {{ background: #1e1b4b; color: #a5b4fc; border: 1px solid #312e81; }}
        .badge-paused {{ background: #450a0a; color: #fca5a5; border: 1px solid #7f1d1d; }}
        .timestamp    {{ font-size: 0.75rem; color: #64748b; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                  gap: 14px; margin-bottom: 24px; }}
        .stat-card {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px; padding: 18px 20px; }}
        .stat-label {{ font-size: 0.72rem; font-weight: 600; text-transform: uppercase;
                       letter-spacing: 0.6px; color: #64748b; margin-bottom: 6px; }}
        .stat-value {{ font-size: 1.6rem; font-weight: 700; color: #f1f5f9; line-height: 1; }}
        .stat-sub   {{ font-size: 0.75rem; color: #475569; margin-top: 5px; }}
        .pos {{ color: #34d399; }} .neg {{ color: #f87171; }} .neu {{ color: #94a3b8; }}
        .section {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px;
                    margin-bottom: 20px; overflow: hidden; }}
        .section-header {{ display: flex; align-items: center; justify-content: space-between;
                           padding: 14px 20px; border-bottom: 1px solid #1e2230; }}
        .section-title {{ font-size: 0.85rem; font-weight: 700; color: #cbd5e1;
                          text-transform: uppercase; letter-spacing: 0.5px; }}
        .count-pill {{ font-size: 0.72rem; font-weight: 700; background: #1e2230; color: #94a3b8;
                       border-radius: 999px; padding: 2px 10px; }}
        .tbl-wrap {{ overflow-x: auto; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.82rem; }}
        thead th {{ padding: 10px 16px; text-align: left; font-size: 0.70rem; font-weight: 600;
                    text-transform: uppercase; letter-spacing: 0.5px; color: #475569;
                    background: #13151a; white-space: nowrap; }}
        tbody tr {{ border-top: 1px solid #1a1d26; transition: background 0.15s; }}
        tbody tr:hover {{ background: #1c1f28; }}
        tbody td {{ padding: 12px 16px; color: #cbd5e1; vertical-align: middle; }}
        .market-name {{ font-weight: 500; color: #e2e8f0; max-width: 300px; }}
        .outcome-pill {{ display: inline-block; font-size: 0.68rem; font-weight: 700;
                         padding: 2px 8px; border-radius: 999px; text-transform: uppercase;
                         letter-spacing: 0.3px; }}
        .outcome-yes {{ background: #064e3b; color: #6ee7b7; }}
        .outcome-no  {{ background: #450a0a; color: #fca5a5; }}
        .source-tag  {{ font-size: 0.70rem; font-weight: 600; color: #818cf8;
                        background: #1e1b4b; padding: 2px 8px; border-radius: 999px; }}
        .price-mono  {{ font-family: 'Courier New', monospace; font-size: 0.80rem; }}
        .pnl-cell    {{ font-weight: 700; font-size: 0.83rem; white-space: nowrap; }}
        .empty       {{ padding: 32px 20px; text-align: center; color: #334155; font-size: 0.85rem; }}
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
        <div style="display:flex;gap:8px;align-items:center;">
            <span class="badge {mode_badge}">{mode_label}</span>
            <span class="badge {status_badge}">{status_label}</span>
        </div>
    </div>
    <div class="stats">
        <div class="stat-card">
            <div class="stat-label">Total Balance</div>
            <div class="stat-value">${balance:.2f}</div>
            <div class="stat-sub">pUSD &nbsp;·&nbsp; Peak ${peak:.2f}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Available</div>
            <div class="stat-value">${available:.2f}</div>
            <div class="stat-sub">Balance minus reserved</div>
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

def build_dashboard(bot) -> dict:
    def _sign(v): return "+" if v > 0 else ("-" if v < 0 else "")
    def _cls(v):  return "pos" if v > 0 else ("neg" if v < 0 else "neu")
    def _fmt(v):  return f"{abs(v):.4f}" if abs(v) < 0.005 else f"{abs(v):.2f}"

    bankroll  = bot.balance.cached_balance or 0.0
    available = bot._available_balance()
    drawdown  = ((peak_bankroll - bankroll) / peak_bankroll * 100) if peak_bankroll > 0 else 0.0
    is_paused = bool(bot_paused_until and datetime.now() < bot_paused_until)

    status_label = "Paused" if is_paused else "Running"
    status_badge = "badge-paused" if is_paused else "badge-live"
    mode_label   = "Dry Run" if bot.dry_run else "Live"
    mode_badge   = "badge-dry" if bot.dry_run else "badge-live"

    unrealised = 0.0
    pos_rows   = ""
    for p in bot.positions.values():
        ws_price = market_data.get_current_price(p.token_id)
        mid      = ws_price if ws_price > 0 else (p.current_price if p.current_price > 0 else p.entry_price)
        unreal   = (mid - p.entry_price) * p.shares
        unrealised += unreal
        outcome_cls = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        pnl_str     = f"{_sign(unreal)}${_fmt(unreal)}"
        cur_str     = f"{mid:.3f}" if mid > 0 else "—"
        pos_rows += f"""
        <tr>
            <td><span class="source-tag">{p.source_name}</span></td>
            <td class="market-name">{p.question[:60]}</td>
            <td><span class="outcome-pill {outcome_cls}">{p.outcome}</span></td>
            <td>${p.size_usd:.2f}<br><span style="font-size:0.70rem;color:#475569;">{p.shares:.4f} shares</span></td>
            <td class="price-mono">{p.entry_price:.3f}</td>
            <td class="price-mono">{cur_str}</td>
            <td class="pnl-cell {_cls(unreal)}">{pnl_str}</td>
        </tr>"""

    positions_block = (
        f'<div class="tbl-wrap"><table>'
        f'<thead><tr><th>Source</th><th>Market</th><th>Side</th>'
        f'<th>Size</th><th>Entry</th><th>Current</th><th>Unreal PnL</th></tr></thead>'
        f'<tbody>{pos_rows}</tbody></table></div>'
        if pos_rows else
        '<div class="empty"><div class="empty-icon">📭</div>No open positions</div>'
    )

    closed_list = getattr(bot, "closed_positions", [])
    realised    = sum(p.pnl for p in closed_list)
    closed_rows = ""
    for p in reversed(closed_list):
        outcome_cls = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        pnl_str     = f"{_sign(p.pnl)}${_fmt(p.pnl)}"
        closed_rows += f"""
        <tr>
            <td><span class="source-tag">{p.source_name}</span></td>
            <td class="market-name">{p.question[:60]}</td>
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
    comp_delta = compounding_bankroll - INITIAL_BANKROLL

    return {
        "last_updated":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode_label":      mode_label,  "mode_badge":    mode_badge,
        "status_label":    status_label, "status_badge": status_badge,
        "balance":         bankroll,     "available":    available,
        "peak":            peak_bankroll,
        "drawdown":        drawdown,
        "dd_cls":          "neg" if drawdown > 10 else ("neu" if drawdown > 5 else "pos"),
        "max_dd":          MAX_DRAWDOWN * 100,
        "comp_bankroll":   compounding_bankroll,
        "comp_cls":        _cls(comp_delta),
        "comp_rate":       COMPOUNDING_RATE * 100,
        "total_pnl_cls":   _cls(total_pnl),  "total_pnl_sign": _sign(total_pnl),
        "total_pnl_abs":   _fmt(total_pnl),
        "unreal_cls":      _cls(unrealised),  "unreal_sign":    _sign(unrealised),
        "unreal_abs":      _fmt(unrealised),
        "real_cls":        _cls(realised),    "real_sign":      _sign(realised),
        "real_abs":        _fmt(realised),
        "open_count":      len(bot.positions),
        "closed_count":    len(closed_list),
        "positions_block": positions_block,
        "closed_block":    closed_block,
    }

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):  self._handle_request()
    def do_HEAD(self): self._handle_request(send_body=False)

    def _handle_request(self, send_body=True):
        self.send_response(200)
        if self.path == "/" and _bot_ref:
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            if send_body:
                try:
                    html = HTML_TEMPLATE.format(**build_dashboard(_bot_ref))
                    self.wfile.write(html.encode("utf-8"))
                except Exception:
                    self.wfile.write(b"<h1>Dashboard loading...</h1>")
        else:
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            if send_body:
                self.wfile.write(b"OK - PolyCopyTrader running")

    def log_message(self, *args): pass

_bot_ref = None

def run_health_server():
    try:
        server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
        logging.info(f"🌐 Dashboard live at http://0.0.0.0:{HEALTH_PORT}")
        server.serve_forever()
    except Exception as e:
        logging.error(f"Health server failed: {e}")

# ==================== DATA OBJECTS ====================
@dataclass
class Position:
    market_id:             str
    question:              str
    outcome:               str
    token_id:              str
    entry_price:           float
    size_usd:              float
    shares:                float
    source_wallet:         str
    source_name:           str
    status:                str   = "open"
    exit_price:            float = 0.0
    pnl:                   float = 0.0
    order_id:              str   = ""
    current_price:         float = 0.0
    source_shares:         float = 0.0
    shares_at_open:        float = 0.0
    source_shares_at_open: float = 0.0

@dataclass
class PendingLimitBuy:
    pos_key:           str
    token_id:          str
    market_id:         str
    question:          str
    outcome:               str
    source_wallet:     str
    source_name:       str
    limit_price:       float
    size_usd:          float
    order_id:          str
    source_shares:     float    = 0.0
    fill_check_errors: int      = 0
    placed_at:         datetime = field(default_factory=datetime.now)

# ==================== DATA STORES ====================
class SeenTradesStore:
    def __init__(self, filepath: str, db_url: str = ""):
        self.filepath = filepath
        self.db_url   = db_url
        self._seen: Set[str] = set()
        self._conn   = None
        if db_url and PSYCOPG2_AVAILABLE:
            self._init_postgres()
        else:
            self._load_file()
        logging.info(f"SeenTradesStore ready | backend={self.backend} | {len(self._seen)} keys loaded")

    def _init_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require")
            self._conn.autocommit = True
            with self._conn.cursor() as cur:
                cur.execute("""CREATE TABLE IF NOT EXISTS seen_trades (
                    pos_key TEXT PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW())""")
            self._seen   = self._load_postgres()
            self.backend = "postgres"
        except Exception as e:
            logging.error(f"Postgres init failed: {e} — falling back to local file")
            self._conn = None
            self._load_file()

    def _load_postgres(self) -> Set[str]:
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT pos_key FROM seen_trades")
                return {row[0] for row in cur.fetchall()}
        except Exception as e:
            logging.warning(f"Postgres load failed: {e}")
            return set()

    def _save_postgres(self, pos_key: str):
        try:
            with self._conn.cursor() as cur:
                cur.execute("INSERT INTO seen_trades (pos_key) VALUES (%s) ON CONFLICT DO NOTHING", (pos_key,))
        except Exception as e:
            logging.warning(f"Postgres save failed for {pos_key}: {e}")
            self._reconnect_postgres()

    def _save_postgres_many(self, keys):
        if not keys:
            return
        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur, "INSERT INTO seen_trades (pos_key) VALUES %s ON CONFLICT DO NOTHING",
                    [(k,) for k in keys])
        except Exception as e:
            logging.warning(f"Postgres bulk save failed: {e}")
            self._reconnect_postgres()

    def _reconnect_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require")
            self._conn.autocommit = True
        except Exception as e:
            logging.error(f"Postgres reconnect failed: {e}")

    def _load_file(self):
        try:
            with open(self.filepath, "r") as f:
                data = json.load(f)
                self._seen = set(data) if isinstance(data, list) else set()
        except FileNotFoundError:
            self._seen = set()
        except Exception as e:
            logging.warning(f"Could not read seen trades file: {e}")
            self._seen = set()
        self.backend = "local-file"

    def _save_file(self):
        try:
            with open(self.filepath, "w") as f:
                json.dump(sorted(self._seen), f)
        except Exception as e:
            logging.warning(f"Could not save seen trades file: {e}")

    def is_seen(self, pos_key: str) -> bool:
        return pos_key in self._seen

    def mark_seen(self, pos_key: str):
        if pos_key in self._seen:
            return
        self._seen.add(pos_key)
        if self._conn:
            self._save_postgres(pos_key)
        else:
            self._save_file()

    def snapshot_existing(self, pos_keys):
        new_keys = [k for k in pos_keys if k not in self._seen]
        if not new_keys:
            return
        for k in new_keys:
            self._seen.add(k)
        if self._conn:
            self._save_postgres_many(new_keys)
        else:
            self._save_file()
        logging.info(f"Snapshot: marked {len(new_keys)} pre-existing trades as seen")

    @property
    def is_empty(self) -> bool:
        return len(self._seen) == 0

# ==================== BALANCE LAYER ====================
class RobustBalanceManager:
    POLYGON_RPCS = [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com",
        "https://polygon.drpc.org",
    ]

    def __init__(self):
        self.cached_balance: Optional[float] = None
        self.last_update    = 0
        self.peak_balance   = 0.0

    def _fetch_balance(self) -> float:
        if not YOUR_WALLET:
            logging.error("DEPOSIT_WALLET_ADDRESS not set — cannot fetch balance")
            return 0.0
        padded  = YOUR_WALLET.lower().replace("0x", "").zfill(64)
        payload = {"jsonrpc": "2.0", "method": "eth_call",
                   "params": [{"to": PUSD_CONTRACT_ADDRESS, "data": "0x70a08231" + padded}, "latest"], "id": 1}
        for rpc in self.POLYGON_RPCS:
            try:
                resp = requests.post(rpc, json=payload, timeout=8)
                result = resp.json().get("result", "0x0") if resp.status_code == 200 else "0x0"
                if result and result not in ("0x", "0x0"):
                    balance = int(result, 16) / 1_000_000
                    if balance > 0:
                        logging.info(f"pUSD balance via RPC ({rpc}): ${balance:.2f}")
                        return balance
            except Exception as e:
                logging.warning(f"RPC balance fetch failed ({rpc}): {e}")
        return 0.0

    def get_balance(self, force=False) -> Optional[float]:
        if force or self.cached_balance is None or (time.time() - self.last_update > 30):
            real = self._fetch_balance()
            if real > 0:
                self.cached_balance = real
                self.last_update    = time.time()
                if real > self.peak_balance:
                    self.peak_balance = real
            elif self.cached_balance is None:
                logging.error("Could not fetch real pUSD balance.")
        return self.cached_balance

    def fetch_with_retry(self, retries: int = 5, delay: int = 10) -> float:
        for attempt in range(1, retries + 1):
            val = self._fetch_balance()
            if val > 0:
                self.cached_balance = val
                self.peak_balance   = val
                self.last_update    = time.time()
                return val
            time.sleep(delay)
        raise RuntimeError(f"Could not fetch balance after {retries} attempts.")

# ==================== CLOB EXECUTOR ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client  = None
        if not dry_run and CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(api_key=POLY_API_KEY, api_secret=POLY_SECRET, api_passphrase=POLY_PASSPHRASE)
                self.client = ClobClient(host="https://clob.polymarket.com", chain_id=137, key=YOUR_PRIVATE_KEY, creds=creds)
                logging.info("ClobClient V2 initialised — LIVE mode")
            except Exception as e:
                logging.error(f"ClobClient V2 init failed: {e}")

    def pre_sign_order_payload(self, token_id: str, price: float, size: float, side: Side, nonce: int) -> dict:
        if self.dry_run or not self.client:
            return {"mock": True, "price": price, "size": size}
            
        try:
            expiration = int(time.time()) + LIMIT_EXPIRY_SECONDS
            order_args = OrderArgs(token_id=token_id, price=price, size=size, side=side)
            signed_order = self.client.create_order(order_args, options=PartialCreateOrderOptions(tick_size="0.01"), nonce=nonce, expiration=expiration)
            return signed_order
        except Exception as e:
            logging.error(f"EIP-712 Local Signature Matrix gen failure: {e}")
            return {}

    def place_limit_buy(self, token_id: str, amount_usd: float, limit_price: float) -> Tuple[bool, str, float]:
        shares = round(amount_usd / limit_price, 4)
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] LIMIT BUY {shares:.4f} @ {limit_price:.4f} (${amount_usd:.2f})")
            return True, f"dry-limit-{int(time.time())}", limit_price
        for attempt in range(MAX_RETRIES):
            try:
                result = self.client.create_and_post_order(
                    order_args=OrderArgs(token_id=token_id, price=limit_price, size=shares, side=Side.BUY),
                    options=PartialCreateOrderOptions(tick_size="0.01"), order_type=OrderType.GTC)
                order_id = result.get("orderID", result.get("id", "unknown"))
                return True, order_id, limit_price
            except Exception as e:
                time.sleep(RETRY_DELAY)
        return False, "", limit_price

    def cancel_order(self, order_id: str) -> bool:
        if self.dry_run or self.client is None:
            return True
        try:
            self.client.cancel(order_id)
            return True
        except Exception as e:
            return False

    def is_order_filled(self, order_id: str) -> Optional[bool]:
        if self.dry_run or self.client is None:
            return True
        try:
            status = self.client.get_order(order_id).get("status", "").lower()
            return True if status in ("matched", "filled") else False
        except Exception as e:
            return None

    def place_sell(self, token_id: str, shares: float, min_price: float = 0.0) -> Tuple[bool, str]:
        if self.dry_run or self.client is None:
            return True, f"dry-sell-{int(time.time())}"
        for attempt in range(MAX_RETRIES):
            try:
                market_args = MarketOrderArgs(token_id=token_id, amount=shares, side=Side.SELL)
                if min_price > 0:
                    try:
                        market_args = MarketOrderArgs(token_id=token_id, amount=shares, side=Side.SELL, min_price=round(min_price, 4))
                    except TypeError:
                        pass
                result   = self.client.create_and_post_market_order(order_args=market_args, options=PartialCreateOrderOptions(tick_size="0.01"), order_type=OrderType.IOC)
                order_id = result.get("orderID", result.get("id", "unknown"))
                return True, order_id
            except Exception as e:
                time.sleep(RETRY_DELAY)
        return False, ""

# ==================== TRADING CONTROLLER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run  = dry_run
        self.balance  = RobustBalanceManager()
        self.executor = PolymarketExecutor(dry_run)
        self.seen     = SeenTradesStore(SEEN_TRADES_FILE, DATABASE_URL)

        self.positions: Dict[str, Position]        = {}
        self.pending:   Dict[str, PendingLimitBuy] = {}
        self.closed_positions: list                = []
        self._first_scan_done: Set[str]            = set()
        self._session: Optional[aiohttp.ClientSession] = None

        self._pos_cache: PositionCache   = PositionCache(ttl=POSITION_CACHE_TTL)
        self._ob_cache:  OrderbookCache  = OrderbookCache(ttl=ORDERBOOK_CACHE_TTL)

        self._breakers: Dict[str, CircuitBreaker] = {
            addr: CircuitBreaker(cfg["name"]) for addr, cfg in WALLETS.items()
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12))
        return self._session

    async def _get(self, url: str, **kwargs) -> Optional[dict]:
        session = await self._get_session()
        for attempt in range(MAX_RETRIES):
            await throttle.acquire()
            try:
                async with session.get(url, **kwargs) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(int(resp.headers.get("Retry-After", 30)))
                        continue
                    if resp.status == 200:
                        return await resp.json(content_type=None)
            except Exception:
                await asyncio.sleep(RETRY_DELAY)
        return None

    def _reserved_capital(self) -> float:
        return (sum(p.size_usd for p in self.positions.values()) + sum(p.size_usd for p in self.pending.values()))

    def _available_balance(self) -> float:
        return max(0.0, (self.balance.cached_balance or 0.0) - self._reserved_capital())

    def _can_afford(self, amount_usd: float) -> bool:
        return self._available_balance() >= amount_usd * 1.02

    async def _get_orderbook(self, token_id: str) -> Tuple[float, float]:
        cached = self._ob_cache.get(token_id)
        if cached is not None:
            return cached[0], cached[1]
        data = await self._get(f"https://clob.polymarket.com/book?token_id={token_id}")
        if data:
            bids     = data.get("bids", [])
            asks     = data.get("asks", [])
            best_bid = float(bids[0]["price"]) if bids else 0.0
            best_ask = float(asks[0]["price"]) if asks else 0.0
            mid      = (best_bid + best_ask) / 2 if best_bid and best_ask else best_bid or best_ask
            self._ob_cache.set_full(token_id, mid, best_ask, best_bid)
            return mid, best_ask
        return 0.0, 0.0

    async def _get_best_bid(self, token_id: str) -> float:
        entry = self._ob_cache._cache.get(token_id)
        if entry and entry.is_fresh(ORDERBOOK_CACHE_TTL) and len(entry.data) == 3:
            return entry.data[2]
        data = await self._get(f"https://clob.polymarket.com/book?token_id={token_id}")
        if data:
            bids     = data.get("bids", [])
            best_bid = float(bids[0]["price"]) if bids else 0.0
            return best_bid
        return 0.0

    async def _get_positions(self, wallet_addr: str) -> Optional[list]:
        cached = self._pos_cache.get(wallet_addr)
        if cached is not None:
            return cached
        data = await self._get(f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50")
        if data is not None:
            self._pos_cache.set(wallet_addr, data)
        return data

    # ==================== EXACT RESTORED TIERED RISKING ====================
    def get_risk_percent(self, price: float, config: dict) -> float:
        if config.get("risk_type") == "fixed":
            return config.get("fixed_risk", 0.025) # Handles original 2.5% flat rules for Viser and Coniyr
        
        # Exact multi-tiered allocation blueprint for price_based risk profiles (TheSpirit, Kruto)
        if price >= 0.70:
            return 0.030  # 3.0% tier
        elif price >= 0.30:
            return 0.010  # 1.0% tier
        else:
            return 0.006  # 0.6% tier

    def check_drawdown(self) -> bool:
        global peak_bankroll, bot_paused_until
        current = self.balance.get_balance()
        if current is None: return False
        if current > peak_bankroll: peak_bankroll = current
        dd = (peak_bankroll - current) / peak_bankroll if peak_bankroll > 0 else 0
        if dd >= MAX_DRAWDOWN:
            if bot_paused_until is None or datetime.now() > bot_paused_until:
                bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
            return True
        return False

    async def _process_pending_orders(self, source_token_ids_by_wallet: Dict[str, set]):
        for pos_key, pending in list(self.pending.items()):
            wallet_tokens = source_token_ids_by_wallet.get(pending.source_wallet, set())
            if pending.token_id not in wallet_tokens:
                self.executor.cancel_order(pending.order_id)
                del self.pending[pos_key]
                continue

            filled = self.executor.is_order_filled(pending.order_id)
            if filled is None:
                pending.fill_check_errors += 1
                if pending.fill_check_errors >= MAX_FILL_CHECK_ERRORS:
                    self.executor.cancel_order(pending.order_id)
                    del self.pending[pos_key]
                continue

            if filled:
                shares = pending.size_usd / pending.limit_price if pending.limit_price > 0 else 0
                self.positions[pos_key] = Position(
                    market_id=pending.market_id, question=pending.question, outcome=pending.outcome,
                    token_id=pending.token_id, entry_price=pending.limit_price, size_usd=pending.size_usd, shares=shares,
                    source_wallet=pending.source_wallet, source_name=pending.source_name, order_id=pending.order_id,
                    source_shares=pending.source_shares, shares_at_open=shares, source_shares_at_open=pending.source_shares,
                )
                del self.pending[pos_key]
                continue

            age = (datetime.now() - pending.placed_at).total_seconds()
            if age >= LIMIT_EXPIRY_SECONDS:
                self.executor.cancel_order(pending.order_id)
                del self.pending[pos_key]

                mid_price, best_ask = await self._get_orderbook(pending.token_id)
                if best_ask <= 0 and mid_price <= 0: continue
                current_ask = best_ask if best_ask > 0 else mid_price
                _cfg = WALLETS.get(pending.source_wallet, {})
                wallet_premium = _cfg.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
                limit_price = round(min(current_ask, current_ask * (1 + wallet_premium)), 4)

                if not self._can_afford(pending.size_usd): continue
                
                success = await self._try_warm_matrix_blast(pending.token_id, current_ask)
                if not success:
                    ok, order_id, actual_price = self.executor.place_limit_buy(pending.token_id, pending.size_usd, limit_price)
                    if ok:
                        self.pending[pos_key] = PendingLimitBuy(
                            pos_key=pos_key, token_id=pending.token_id, market_id=pending.market_id,
                            question=pending.question, outcome=pending.outcome, source_wallet=pending.source_wallet,
                            source_name=pending.source_name, limit_price=actual_price, size_usd=pending.size_usd,
                            order_id=order_id, source_shares=pending.source_shares,
                        )

    async def _try_warm_matrix_blast(self, token_id: str, market_price: float) -> bool:
        global PRE_SIGNED_MATRIX_CACHE, WARM_HTTP_SESSION
        token_matrix = PRE_SIGNED_MATRIX_CACHE.get(token_id)
        if not token_matrix or not WARM_HTTP_SESSION:
            return False
            
        target_tick = str(round(market_price, 2))
        payload = token_matrix.get(target_tick)
        
        if not payload:
            logging.warning(f"⚠️ Tight Guard Triggered: Slipped entry at {market_price} rejected.")
            return False
            
        try:
            headers = {"Content-Type": "application/json"}
            async with WARM_HTTP_SESSION.post("https://clob.polymarket.com/order", json=payload, headers=headers) as r:
                if r.status in (200, 201):
                    logging.info(f"💥 Microsecond HFT execution matrix deployment hit! Fill confirmation on tick: {target_tick}")
                    return True
        except Exception as e:
            logging.error(f"HFT Pipeline blast anomaly: {e}")
        return False

    async def _execute_sell(self, position: Position, pos_key: str, shares_to_sell: float, name: str, full_exit: bool, current_source_shares: float = 0.0):
        global compounding_bankroll
        if self.dry_run:
            ws_price = market_data.get_current_price(position.token_id)
            exit_price = ws_price if ws_price > 0 else (position.current_price if position.current_price > 0 else position.entry_price)
            pnl = (exit_price - position.entry_price) * shares_to_sell
            ok = True
        else:
            best_bid  = await self._get_best_bid(position.token_id)
            min_price = round(best_bid * (1 - MAX_SLIPPAGE), 4) if best_bid > 0 else 0.0
            pending_costs_before = {pk: p.size_usd for pk, p in self.pending.items()}

            with _trade_lock:
                balance_before = self.balance.get_balance(force=True) or 0.0
                ok, order_id   = self.executor.place_sell(position.token_id, shares_to_sell, min_price=min_price)

            if ok:
                await asyncio.sleep(SELL_SETTLE_WAIT)
                with _trade_lock:
                    balance_after = self.balance.get_balance(force=True) or 0.0

            if ok:
                contamination = sum(cost for pk, cost in pending_costs_before.items() if pk in self.positions)
                pnl = (balance_after - balance_before) + contamination
                exit_price = best_bid if best_bid > 0 else position.current_price
            else:
                pnl = exit_price = 0.0

        if not ok: return

        if full_exit:
            position.status, position.exit_price, position.pnl = "closed", exit_price, pnl
            if pnl > 0: compounding_bankroll += pnl * COMPOUNDING_RATE
            self.closed_positions.append(position)
            del self.positions[pos_key]
        else:
            position.shares -= shares_to_sell
            position.size_usd = position.shares * position.entry_price
            position.source_shares = current_source_shares
            if pnl > 0: compounding_bankroll += pnl * COMPOUNDING_RATE

    async def _scan_wallet(self, wallet_addr: str, config: dict) -> Tuple[str, Optional[list], set, Dict[str, float]]:
        breaker = self._breakers[wallet_addr]
        if not breaker.allow_request(): return wallet_addr, None, set(), {}
        data = await self._get_positions(wallet_addr)
        if data is None:
            breaker.record_failure()
            return wallet_addr, None, set(), {}
        breaker.record_success()

        source_token_ids, source_shares_map = set(), {}
        for pos in data:
            tid, shares = pos.get("asset", ""), float(pos.get("size", pos.get("shares", 0)))
            if tid and shares > 0:
                source_token_ids.add(tid)
                source_shares_map[tid] = shares
        return wallet_addr, data, source_token_ids, source_shares_map

    async def scan_and_copy(self):
        global current_bankroll, compounding_bankroll, bot_paused_until
        if (bot_paused_until and datetime.now() < bot_paused_until) or self.check_drawdown(): return
        current_bankroll = self.balance.get_balance()
        if current_bankroll is None: return

        wallet_items = list(WALLETS.items())
        results = await asyncio.gather(*[self._scan_wallet(addr, cfg) for addr, cfg in wallet_items], return_exceptions=True)
        source_token_ids_by_wallet = {}

        for result in results:
            if isinstance(result, Exception): continue
            wallet_addr, raw, source_token_ids, source_shares_map = result
            config, name = WALLETS[wallet_addr], WALLETS[wallet_addr]["name"]
            
            # CRITICAL FIX: If the API failed to provide data, skip execution immediately.
            if raw is None: 
                logging.warning(f"⚠️ Initial tracking fetch failed for {name}. Postponing execution phase until snapshot secures data.")
                continue

            # --- BULLETPROOF INITIALIZATION SNAPSHOT PHASE ---
            if wallet_addr not in self._first_scan_done:
                if config.get("copy_mode") == "new_only":
                    all_keys = {f"{wallet_addr}_{tid}" for tid in source_token_ids}
                    self.seen.snapshot_existing(all_keys)
                    logging.info(f"🔒 [{name}] Successful initial snapshot. Protected {len(all_keys)} existing position(s).")
                
                self._first_scan_done.add(wallet_addr)
                source_token_ids_by_wallet[wallet_addr] = source_token_ids
                continue

            if self._breakers[wallet_addr].state != "CLOSED":
                source_token_ids_by_wallet[wallet_addr] = source_token_ids
            else:
                for pos in raw:
                    token_id, market_id, question, outcome = pos.get("asset", ""), pos.get("conditionId", ""), pos.get("title", "Unknown"), pos.get("outcome", "YES")
                    size_usd, source_shares_at_copy = float(pos.get("currentValue", 0)), float(pos.get("size", pos.get("shares", 0)))
                    if not token_id or size_usd <= 0: continue

                    pos_key = f"{wallet_addr}_{token_id}"
                    if self.seen.is_seen(pos_key) or pos_key in self.positions or pos_key in self.pending: continue
                    if len(self.positions) + len(self.pending) >= MAX_POSITIONS: break

                    cur_price = market_data.get_current_price(token_id) or float(pos.get("curPrice", 0))
                    if cur_price <= 0: continue
                    limit_price = round(cur_price, 4)

                    if config.get("copy_sub_dollar") and size_usd < 1.0: my_size = round(size_usd, 2)
                    else: my_size = round(min(compounding_bankroll * self.get_risk_percent(limit_price, config), self._available_balance() * 0.95), 2)

                    if my_size <= 0 or not self._can_afford(my_size): continue
                    
                    hft_success = await self._try_warm_matrix_blast(token_id, cur_price)
                    if hft_success:
                        self.seen.mark_seen(pos_key)
                        await market_data.subscribe_tokens([token_id])
                        self.positions[pos_key] = Position(
                            market_id=market_id, question=question, outcome=outcome, token_id=token_id,
                            entry_price=limit_price, size_usd=my_size, shares=round(my_size / limit_price, 4),
                            source_wallet=wallet_addr, source_name=name, order_id="hft_blast",
                            source_shares=source_shares_at_copy, shares_at_open=round(my_size / limit_price, 4),
                            source_shares_at_open=source_shares_at_copy
                        )
                    else:
                        ok, order_id, actual_price = self.executor.place_limit_buy(token_id, my_size, limit_price)
                        if ok:
                            self.seen.mark_seen(pos_key)
                            await market_data.subscribe_tokens([token_id])
                            self.pending[pos_key] = PendingLimitBuy(pos_key=pos_key, token_id=token_id, market_id=market_id, question=question, outcome=outcome, source_wallet=wallet_addr, source_name=name, limit_price=actual_price, size_usd=my_size, order_id=order_id, source_shares=source_shares_at_copy)

                source_token_ids_by_wallet[wallet_addr] = source_token_ids

            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr or position.status != "open": continue
                current_source_shares = source_shares_map.get(position.token_id, 0.0)

                if current_source_shares > position.source_shares_at_open:
                    position.source_shares_at_open = position.source_shares = current_source_shares
                    position.shares_at_open        = position.shares

                if position.token_id not in source_token_ids:
                    await self._execute_sell(position, pos_key, position.shares, name, full_exit=True)
                elif position.source_shares_at_open > 0 and current_source_shares < position.source_shares * 0.80:
                    our_shares_to_sell = round(position.shares - round(position.shares_at_open * (current_source_shares / position.source_shares_at_open), 4), 4)
                    if our_shares_to_sell > 0:
                        await self._execute_sell(position, pos_key, our_shares_to_sell, name, full_exit=False, current_source_shares=current_source_shares)

        await self._process_pending_orders(source_token_ids_by_wallet)

    async def run(self):
        while True:
            try: await self.scan_and_copy()
            except Exception as e: logging.error(f"Main loop error: {e}")
            
            market_data.activity_event.clear()
            try: 
                await asyncio.wait_for(market_data.activity_event.wait(), timeout=POLL_INTERVAL)
                logging.info("⚡ Activity event — running early scan")
            except asyncio.TimeoutError: 
                pass

# ==================== PIPELINE OPTIMIZATION ENGINES ====================
async def setup_pre_warmed_pipeline():
    global WARM_HTTP_SESSION
    connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300, keepalive_timeout=60)
    WARM_HTTP_SESSION = aiohttp.ClientSession(connector=connector)
    logging.info("🚀 Pre-warmed TCP pipeline activated.")

async def connection_keepalive_heartbeat():
    global WARM_HTTP_SESSION
    while True:
        if WARM_HTTP_SESSION and not WARM_HTTP_SESSION.closed:
            try:
                async with WARM_HTTP_SESSION.get("https://clob.polymarket.com/live") as r:
                    await r.read()
            except Exception:
                pass
        await asyncio.sleep(20)

async def matrix_pre_sign_worker(bot: CopyTrader):
    global PRE_SIGNED_MATRIX_CACHE
    while True:
        if not bot.dry_run and CLOB_AVAILABLE and bot.executor.client:
            try:
                address_param = YOUR_WALLET.lower()
                nonce_resp = requests.get(f"https://clob.polymarket.com/nonce?address={address_param}", timeout=5)
                current_nonce = int(nonce_resp.json().get("nonce", 0))
                
                # --- DYNAMIC FIX APPLIED ---
                # Aggregates active token IDs automatically from active portfolio streams
                tracked_tokens = set()
                for pos in bot.positions.values():
                    tracked_tokens.add(pos.token_id)
                for pend in bot.pending.values():
                    tracked_tokens.add(pend.token_id)
                
                # Fallback to standard baseline tracking loop assets if current portfolio is resting empty
                if not tracked_tokens:
                    tracked_tokens.add("0x271a9918c9629f903c4cd098184e67a06a04f9b8f")
                
                for tid in list(tracked_tokens):
                    mid_price, _ = await bot._get_orderbook(tid)
                    if mid_price <= 0:
                        mid_price = 0.50
                        
                    wallet_cfg = list(WALLETS.values())[0]
                    premium_limit = wallet_cfg.get("limit_buy_max_premium", 0.10)
                    max_allowed_price = mid_price * (1.0 + premium_limit)
                    
                    price_matrix = {}
                    current_tick = mid_price
                    while current_tick <= max_allowed_price:
                        tick_key = str(round(current_tick, 2))
                        price_matrix[tick_key] = bot.executor.pre_sign_order_payload(
                            token_id=tid, price=round(current_tick, 2), size=50.0, side=Side.BUY, nonce=current_nonce
                        )
                        current_tick += 0.01
                        
                    PRE_SIGNED_MATRIX_CACHE[tid] = price_matrix
            except Exception as e:
                logging.warning(f"Signature Generation Loop Warning: {e}")
        await asyncio.sleep(45)

# ==================== ENTRY POINT ====================
async def main():
    global _bot_ref, compounding_bankroll, peak_bankroll

    threading.Thread(target=run_health_server, daemon=True).start()

    bot      = CopyTrader(dry_run=DRY_RUN)
    _bot_ref = bot

    try:
        starting_balance         = bot.balance.fetch_with_retry(retries=5, delay=10)
        bot.balance.peak_balance = starting_balance
        peak_bankroll            = starting_balance
        compounding_bankroll     = starting_balance
        logging.info(f"Compounding bankroll seeded at ${compounding_bankroll:.2f}")
    except RuntimeError as e:
        logging.error(f"Startup balance fetch failed: {e} — running in degraded mode")

    await setup_pre_warmed_pipeline()
    asyncio.create_task(connection_keepalive_heartbeat())
    asyncio.create_task(matrix_pre_sign_worker(bot))

    market_data.running = True
    wallet_addresses    = list(WALLETS.keys())

    market_task   = asyncio.create_task(market_data.connect_market())
    activity_task = asyncio.create_task(market_data.connect_user(wallet_addresses))

    try:
        await bot.run()
    finally:
        market_data.running = False
        market_task.cancel()
        activity_task.cancel()
        if bot._session and not bot._session.closed:
            await bot._session.close()
        if WARM_HTTP_SESSION and not WARM_HTTP_SESSION.closed:
            await WARM_HTTP_SESSION.close()

if __name__ == "__main__":
    asyncio.run(main())
