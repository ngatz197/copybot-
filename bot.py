#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - PRODUCTION READY (MERGED)

Merges:
  - Bot 1 base structure + WebSocket market-data feed
  - Bot 2 full dashboard (stat cards, PnL, closed trades, badges)
  - Bot 2 SeenTradesStore (Postgres + local-file fallback)
  - Bot 2 PolymarketExecutor (CLOB V2, retries, slippage guard)
  - Bot 2 affordability / drawdown / peak-bankroll logic
  - Bot 2 MAX_FILL_CHECK_ERRORS (no silent order drops)
  - Bot 2 partial-sell + PnL contamination correction
  - Bot 2 heartbeat logging, rate-limit handling, per-wallet premium

Additional changes:
  - Poll interval fixed at 15s (REST fallback safety net)
  - WS user-activity feed subscribes to all source wallet addresses;
    any detected trade event immediately wakes the scan loop so buys
    and sells are mirrored in near real-time without waiting for the
    poll timer. REST poll remains as a fallback / reconciliation pass.
  - WS wakeup debounce: 2s coalescing window prevents redundant scans
    from rapid-fire activity events.
  - aiohttp replaces requests for all REST calls (non-blocking)
  - All wallet position fetches run simultaneously via asyncio.gather()
  - Global async request throttle (max 2 req/s) prevents rate limiting
  - Position cache (12s TTL): rapid re-scans reuse cached responses
  - Orderbook cache (3s TTL): avoids redundant fetches within a window
  - Sell loop no longer calls _get_best_bid() per position per cycle;
    orderbook is only fetched at the moment a sell is actually executed
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
import aiohttp
import requests   # kept only for RobustBalanceManager RPC calls (sync ok there)

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
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
POLY_API_KEY     = os.getenv("POLY_API_KEY", "")
POLY_SECRET      = os.getenv("POLY_SECRET", "")
POLY_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")
DATABASE_URL     = os.getenv("DATABASE_URL", "")

INITIAL_BANKROLL      = 10.0
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "20"))
POLL_INTERVAL         = 15   # fixed — do not override via env
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

# Throttle: minimum seconds between outbound REST requests globally.
MIN_REQUEST_GAP = float(os.getenv("MIN_REQUEST_GAP", "0.5"))

# Cache TTLs
POSITION_CACHE_TTL  = 12   # seconds — less than poll interval
ORDERBOOK_CACHE_TTL = 3    # seconds — very short, prices move fast

# WS debounce: coalesce rapid activity events into a single scan
WS_DEBOUNCE_SECONDS = 2.0

PUSD_CONTRACT_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

current_bankroll      = INITIAL_BANKROLL
peak_bankroll         = INITIAL_BANKROLL
compounding_bankroll  = INITIAL_BANKROLL
bot_paused_until: Optional[datetime] = None

_trade_lock = threading.Lock()


# ==================== GLOBAL ASYNC THROTTLE ====================
class RequestThrottle:
    """
    Enforces a minimum gap between outbound aiohttp requests globally.
    All callers await throttle.acquire() before firing a request.
    """
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


# ==================== POSITION CACHE ====================
@dataclass
class CacheEntry:
    data:       object
    fetched_at: float = field(default_factory=time.monotonic)

    def is_fresh(self, ttl: float) -> bool:
        return (time.monotonic() - self.fetched_at) < ttl


class PositionCache:
    """
    Per-wallet cache with a 12s TTL.
    Rapid re-scans (triggered by WS wakeups) reuse the cached response
    instead of hammering the data API on every activity event.
    """
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
    """
    Per-token orderbook cache with a 3s TTL.
    Prevents redundant fetches when the same token appears across
    multiple positions or when a retry fires within the same scan.
    """
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
        """Returns cached best_ask as a bid proxy (same fetch), or None."""
        cached = self.get(token_id)
        # orderbook cache stores (mid, best_ask); best bid needs a separate fetch
        # but we also store bid separately when fetched for sells
        entry = self._cache.get(token_id)
        if entry and entry.is_fresh(self._ttl) and len(entry.data) == 3:
            return entry.data[2]
        return None

    def set_full(self, token_id: str, mid: float, best_ask: float, best_bid: float):
        self._cache[token_id] = CacheEntry(data=(mid, best_ask, best_bid))


# ==================== MARKET DATA (two-channel WS) ====================
class MarketDataManager:
    """
    Channel 1 — market prices (wss://.../ws/market)
    Channel 2 — user activity (wss://.../ws/user)
        Activity events are debounced: the first event sets a short timer;
        subsequent events within WS_DEBOUNCE_SECONDS are swallowed.
        When the timer expires, activity_event is set once so the scan
        loop wakes exactly once per burst of activity.
    """

    def __init__(self):
        self.token_to_price:    Dict[str, float] = {}
        self.subscribed_tokens: Set[str]         = set()
        self.running                             = False
        self.activity_event                      = asyncio.Event()
        self._subscribed_wallets: Set[str]       = set()
        self._market_ws                          = None
        self._user_ws                            = None
        self._debounce_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Channel 1: market prices
    # ------------------------------------------------------------------
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
                logging.info(f"Market-price WS: subscribed {len(new)} new token(s)")
            except Exception as e:
                logging.warning(f"Market-price subscription failed: {e}")

    # ------------------------------------------------------------------
    # Channel 2: user activity  (with debounce)
    # ------------------------------------------------------------------
    async def _debounce_wake(self):
        """
        Wait WS_DEBOUNCE_SECONDS, then fire activity_event once.
        Any further events that arrive during the wait are swallowed
        because _debounce_task is already running.
        """
        await asyncio.sleep(WS_DEBOUNCE_SECONDS)
        self.activity_event.set()
        self._debounce_task = None

    def _schedule_wake(self, wallet: str, event_type: str):
        """Called on every relevant WS message. Starts debounce if not running."""
        if self._debounce_task is None or self._debounce_task.done():
            logging.info(
                f"🔔 Activity WS | wallet={wallet[:10]}… "
                f"type={event_type} — debounce {WS_DEBOUNCE_SECONDS}s"
            )
            self._debounce_task = asyncio.create_task(self._debounce_wake())
        # else: debounce already ticking — this event is coalesced silently

    async def connect_user(self, wallet_addresses: list):
        uri        = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
        wallet_set = {w.lower() for w in wallet_addresses}
        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=30) as ws:
                    self._user_ws = ws
                    logging.info("✅ User-activity WS connected")
                    await self._send_user_sub(ws, wallet_addresses)
                    self._subscribed_wallets.update(wallet_addresses)
                    async for message in ws:
                        try:
                            data = json.loads(message)
                            event_type = (
                                data.get("type") or
                                data.get("event_type") or
                                data.get("action", "")
                            )
                            user = (
                                data.get("user") or
                                data.get("maker") or
                                data.get("taker", "")
                            )
                            if user.lower() in wallet_set:
                                self._schedule_wake(user, event_type)
                            elif event_type:
                                self._schedule_wake("unknown", event_type)
                        except Exception:
                            pass
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


# ==================== DASHBOARD ====================
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


# ==================== HEALTH SERVER ====================
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


# ==================== DATA CLASSES ====================
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
    outcome:           str
    source_wallet:     str
    source_name:       str
    limit_price:       float
    size_usd:          float
    order_id:          str
    source_shares:     float    = 0.0
    fill_check_errors: int      = 0
    placed_at:         datetime = field(default_factory=datetime.now)


# ==================== SEEN TRADES STORE ====================
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


# ==================== BALANCE MANAGER ====================
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
                resp   = requests.post(rpc, json=payload, timeout=8)
                result = resp.json().get("result", "0x0") if resp.status_code == 200 else "0x0"
                if result and result not in ("0x", "0x0"):
                    balance = int(result, 16) / 1_000_000
                    if balance > 0:
                        logging.info(f"pUSD balance via RPC ({rpc}): ${balance:.2f}")
                        return balance
                    logging.warning(f"pUSD balance is 0 for {YOUR_WALLET[:10]}…")
            except Exception as e:
                logging.warning(f"RPC balance fetch failed ({rpc}): {e}")
        logging.error(f"All RPC attempts failed for {YOUR_WALLET[:10] if YOUR_WALLET else 'NOT SET'}…")
        return 0.0

    def get_balance(self, force=False) -> Optional[float]:
        if force or self.cached_balance is None or (time.time() - self.last_update > 30):
            real = self._fetch_balance()
            if real > 0:
                self.cached_balance = real
                self.last_update    = time.time()
                if real > self.peak_balance:
                    self.peak_balance = real
                    logging.info(f"New peak balance: ${self.peak_balance:.2f}")
            elif self.cached_balance is None:
                logging.error("Could not fetch real pUSD balance — bot will not trade until confirmed")
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
            logging.warning(f"Balance fetch attempt {attempt}/{retries} returned 0 — retrying in {delay}s")
            time.sleep(delay)
        raise RuntimeError(f"Could not fetch real pUSD balance after {retries} attempts.")

    def check_drawdown(self) -> Tuple[bool, float]:
        current = self.get_balance()
        if current is None or self.peak_balance == 0:
            return False, 0.0
        dd = (self.peak_balance - current) / self.peak_balance
        return dd >= MAX_DRAWDOWN, dd


# ==================== EXECUTOR ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client  = None
        if not dry_run and CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(api_key=POLY_API_KEY, api_secret=POLY_SECRET, api_passphrase=POLY_PASSPHRASE)
                self.client = ClobClient(host="https://clob.polymarket.com", chain_id=137,
                                         key=YOUR_PRIVATE_KEY, creds=creds)
                logging.info("ClobClient V2 initialised — LIVE mode")
            except Exception as e:
                logging.error(f"ClobClient V2 init failed: {e}")

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
                logging.info(f"LIMIT BUY placed: {order_id} | {shares:.4f} @ {limit_price:.4f}")
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

    def is_order_filled(self, order_id: str) -> Optional[bool]:
        """True=filled, False=confirmed open, None=API error (do NOT drop order)."""
        if self.dry_run or self.client is None:
            return True
        try:
            status = self.client.get_order(order_id).get("status", "").lower()
            return True if status in ("matched", "filled") else False
        except Exception as e:
            logging.warning(f"Fill-check API error for {order_id}: {e} — treating as unknown")
            return None

    def place_sell(self, token_id: str, shares: float, min_price: float = 0.0) -> Tuple[bool, str]:
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] MARKET SELL {shares:.4f} shares min_price={min_price:.4f}")
            return True, f"dry-sell-{int(time.time())}"
        for attempt in range(MAX_RETRIES):
            try:
                market_args = MarketOrderArgs(token_id=token_id, amount=shares, side=Side.SELL)
                if min_price > 0:
                    try:
                        market_args = MarketOrderArgs(token_id=token_id, amount=shares,
                                                      side=Side.SELL, min_price=round(min_price, 4))
                    except TypeError:
                        pass
                result   = self.client.create_and_post_market_order(
                    order_args=market_args,
                    options=PartialCreateOrderOptions(tick_size="0.01"), order_type=OrderType.IOC)
                order_id = result.get("orderID", result.get("id", "unknown"))
                logging.info(f"MARKET SELL placed (IOC): {order_id} min_price={min_price:.4f}")
                return True, order_id
            except Exception as e:
                logging.warning(f"SELL attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return False, ""


# ==================== COPY TRADER ====================
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

        # aiohttp session — created once, reused for all REST calls
        self._session: Optional[aiohttp.ClientSession] = None

        # Caches
        self._pos_cache: PositionCache   = PositionCache(ttl=POSITION_CACHE_TTL)
        self._ob_cache:  OrderbookCache  = OrderbookCache(ttl=ORDERBOOK_CACHE_TTL)

        logging.info(f"CopyTrader started | mode={'DRY RUN' if dry_run else 'LIVE'}")
        logging.info(
            f"Watching {len(WALLETS)} wallets | max_positions={MAX_POSITIONS} | "
            f"ask_cap=+{LIMIT_BUY_MAX_PREMIUM*100:.0f}% | max_slippage={MAX_SLIPPAGE*100:.0f}% | "
            f"expiry={LIMIT_EXPIRY_SECONDS}s | poll={POLL_INTERVAL}s | throttle={MIN_REQUEST_GAP}s | "
            f"pos_cache={POSITION_CACHE_TTL}s | ob_cache={ORDERBOOK_CACHE_TTL}s | "
            f"ws_debounce={WS_DEBOUNCE_SECONDS}s | "
            f"storage={self.seen.backend} | collateral=pUSD"
        )
        for addr, cfg in WALLETS.items():
            logging.info(f"  {cfg['name']} ({addr[:10]}…) copy_mode={cfg['copy_mode']}")

    # ------------------------------------------------------------------
    # aiohttp session lifecycle
    # ------------------------------------------------------------------
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=12)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    # ------------------------------------------------------------------
    # Throttled async GET
    # ------------------------------------------------------------------
    async def _get(self, url: str, **kwargs) -> Optional[dict]:
        session = await self._get_session()
        for attempt in range(MAX_RETRIES):
            await throttle.acquire()
            try:
                async with session.get(url, **kwargs) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 30))
                        logging.warning(f"Rate limited on {url[:50]} — sleeping {retry_after}s")
                        await asyncio.sleep(retry_after)
                        continue
                    if resp.status == 200:
                        return await resp.json(content_type=None)
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    logging.warning(f"GET failed ({url[:50]}): {e}")
                await asyncio.sleep(RETRY_DELAY)
        return None

    # ------------------------------------------------------------------
    # Capital helpers
    # ------------------------------------------------------------------
    def _reserved_capital(self) -> float:
        return (sum(p.size_usd for p in self.positions.values()) +
                sum(p.size_usd for p in self.pending.values()))

    def _available_balance(self) -> float:
        return max(0.0, (self.balance.cached_balance or 0.0) - self._reserved_capital())

    def _can_afford(self, amount_usd: float) -> bool:
        available = self._available_balance()
        can       = available >= amount_usd * 1.02
        if not can:
            logging.warning(
                f"Affordability check failed: need ${amount_usd:.2f} | "
                f"available=${available:.2f} | reserved=${self._reserved_capital():.2f}")
        return can

    # ------------------------------------------------------------------
    # Async orderbook helpers — cache-aware
    # ------------------------------------------------------------------
    async def _get_orderbook(self, token_id: str) -> Tuple[float, float]:
        """Returns (mid_price, best_ask). Checks cache first; fetches and caches on miss."""
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
            # Store full (mid, ask, bid) so _get_best_bid can reuse the same entry
            self._ob_cache.set_full(token_id, mid, best_ask, best_bid)
            return mid, best_ask
        return 0.0, 0.0

    async def _get_best_bid(self, token_id: str) -> float:
        """Returns best bid price. Reuses cached orderbook when fresh."""
        # Try full cache entry first
        entry = self._ob_cache._cache.get(token_id)
        if entry and entry.is_fresh(ORDERBOOK_CACHE_TTL) and len(entry.data) == 3:
            return entry.data[2]   # (mid, ask, bid)[2]

        # Fall back to a fresh fetch (also populates cache for mid/ask)
        data = await self._get(f"https://clob.polymarket.com/book?token_id={token_id}")
        if data:
            bids     = data.get("bids", [])
            asks     = data.get("asks", [])
            best_bid = float(bids[0]["price"]) if bids else 0.0
            best_ask = float(asks[0]["price"]) if asks else 0.0
            mid      = (best_bid + best_ask) / 2 if best_bid and best_ask else best_bid or best_ask
            self._ob_cache.set_full(token_id, mid, best_ask, best_bid)
            return best_bid
        return 0.0

    # ------------------------------------------------------------------
    # Async position fetch — cache-aware
    # ------------------------------------------------------------------
    async def _get_positions(self, wallet_addr: str) -> Optional[list]:
        """
        Returns positions for wallet_addr.
        Returns cached data if it is still within POSITION_CACHE_TTL seconds old,
        otherwise fetches fresh data and updates the cache.
        """
        cached = self._pos_cache.get(wallet_addr)
        if cached is not None:
            return cached

        data = await self._get(
            f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50"
        )
        if data is not None:
            self._pos_cache.set(wallet_addr, data)
        return data

    # ------------------------------------------------------------------
    # Risk sizing
    # ------------------------------------------------------------------
    def get_risk_percent(self, price: float, config: dict) -> float:
        if config.get("risk_type") == "fixed":
            return config.get("fixed_risk", 0.025)
        if price >= 0.70:   return 0.03
        elif price >= 0.30: return 0.01
        else:               return 0.006

    # ------------------------------------------------------------------
    # Drawdown guard
    # ------------------------------------------------------------------
    def check_drawdown(self) -> bool:
        global peak_bankroll, bot_paused_until
        current = self.balance.get_balance()
        if current is None:
            return False
        if current > peak_bankroll:
            peak_bankroll = current
        dd = (peak_bankroll - current) / peak_bankroll if peak_bankroll > 0 else 0
        if dd >= MAX_DRAWDOWN:
            if bot_paused_until is None or datetime.now() > bot_paused_until:
                bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
                logging.warning(f"DRAWDOWN PROTECTION TRIGGERED ({dd*100:.1f}%) — paused {PAUSE_HOURS}h")
            return True
        return False

    # ------------------------------------------------------------------
    # Pending order management
    # ------------------------------------------------------------------
    async def _process_pending_orders(self, source_token_ids_by_wallet: Dict[str, set]):
        for pos_key, pending in list(self.pending.items()):
            wallet_tokens = source_token_ids_by_wallet.get(pending.source_wallet, set())

            if pending.token_id not in wallet_tokens:
                logging.info(f"Source exited before fill — cancelling {pending.question[:40]}")
                self.executor.cancel_order(pending.order_id)
                del self.pending[pos_key]
                continue

            filled = self.executor.is_order_filled(pending.order_id)

            if filled is None:
                pending.fill_check_errors += 1
                logging.warning(f"Fill check error #{pending.fill_check_errors} for {pending.question[:40]}")
                if pending.fill_check_errors >= MAX_FILL_CHECK_ERRORS:
                    logging.error(f"Max fill check errors reached for {pending.question[:40]} — cancelling")
                    self.executor.cancel_order(pending.order_id)
                    del self.pending[pos_key]
                continue

            if filled:
                pending.fill_check_errors = 0
                shares = pending.size_usd / pending.limit_price if pending.limit_price > 0 else 0
                self.positions[pos_key] = Position(
                    market_id=pending.market_id, question=pending.question, outcome=pending.outcome,
                    token_id=pending.token_id, entry_price=pending.limit_price,
                    size_usd=pending.size_usd, shares=shares,
                    source_wallet=pending.source_wallet, source_name=pending.source_name,
                    order_id=pending.order_id, source_shares=pending.source_shares,
                    shares_at_open=shares, source_shares_at_open=pending.source_shares,
                )
                del self.pending[pos_key]
                logging.info(f"LIMIT BUY FILLED → position open | {pending.question[:40]} @ {pending.limit_price:.4f}")
                continue

            age = (datetime.now() - pending.placed_at).total_seconds()
            if age >= LIMIT_EXPIRY_SECONDS:
                logging.info(f"Order expired ({age:.0f}s) — retrying {pending.question[:40]}")
                self.executor.cancel_order(pending.order_id)
                del self.pending[pos_key]

                # Orderbook cache will serve fresh price on retry if available
                mid_price, best_ask = await self._get_orderbook(pending.token_id)
                if best_ask <= 0 and mid_price <= 0:
                    logging.info(f"No orderbook on retry — skipping {pending.question[:40]}")
                    continue

                current_ask    = best_ask if best_ask > 0 else mid_price
                _cfg           = WALLETS.get(pending.source_wallet, {})
                wallet_premium = _cfg.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
                limit_price    = round(min(current_ask, current_ask * (1 + wallet_premium)), 4)

                if not self._can_afford(pending.size_usd):
                    logging.warning(f"Cannot afford retry for {pending.question[:40]} — skipping")
                    continue

                ok, order_id, actual_price = self.executor.place_limit_buy(
                    pending.token_id, pending.size_usd, limit_price)
                if ok:
                    self.pending[pos_key] = PendingLimitBuy(
                        pos_key=pos_key, token_id=pending.token_id, market_id=pending.market_id,
                        question=pending.question, outcome=pending.outcome,
                        source_wallet=pending.source_wallet, source_name=pending.source_name,
                        limit_price=actual_price, size_usd=pending.size_usd,
                        order_id=order_id, source_shares=pending.source_shares,
                    )
                    logging.info(f"LIMIT BUY RETRIED | {pending.question[:40]} @ {actual_price:.4f}")

    # ------------------------------------------------------------------
    # Sell execution
    # ------------------------------------------------------------------
    async def _execute_sell(
        self,
        position:              Position,
        pos_key:               str,
        shares_to_sell:        float,
        name:                  str,
        full_exit:             bool,
        current_source_shares: float = 0.0,
    ):
        global compounding_bankroll

        if self.dry_run:
            ws_price   = market_data.get_current_price(position.token_id)
            exit_price = ws_price if ws_price > 0 else (
                position.current_price if position.current_price > 0 else position.entry_price)
            pnl      = (exit_price - position.entry_price) * shares_to_sell
            ok       = True
            order_id = f"dry-sell-{int(time.time())}"
        else:
            # _get_best_bid reuses orderbook cache if fresh (3s TTL)
            best_bid  = await self._get_best_bid(position.token_id)
            min_price = round(best_bid * (1 - MAX_SLIPPAGE), 4) if best_bid > 0 else 0.0

            pending_costs_before: Dict[str, float] = {pk: p.size_usd for pk, p in self.pending.items()}

            with _trade_lock:
                balance_before = self.balance.get_balance(force=True) or 0.0
                ok, order_id   = self.executor.place_sell(position.token_id, shares_to_sell, min_price=min_price)
                if ok:
                    time.sleep(SELL_SETTLE_WAIT)
                    balance_after = self.balance.get_balance(force=True) or 0.0

            if ok:
                contamination = sum(cost for pk, cost in pending_costs_before.items() if pk in self.positions)
                if contamination:
                    logging.info(f"PnL contamination correction: +${contamination:.4f}")
                pnl        = (balance_after - balance_before) + contamination
                exit_price = best_bid if best_bid > 0 else position.current_price
            else:
                pnl = exit_price = 0.0

        if not ok:
            logging.error(f"[{name}] SELL failed — will retry next poll: {position.question[:40]}")
            return

        if full_exit:
            position.status     = "closed"
            position.exit_price = exit_price
            position.pnl        = pnl
            if pnl > 0:
                compounding_bankroll += pnl * COMPOUNDING_RATE
                logging.info(f"Compounding profit: +${pnl * COMPOUNDING_RATE:.4f} → ${compounding_bankroll:.2f}")
            logging.info(
                f"[{name}] FULL SELL ({'DRY RUN' if self.dry_run else 'LIVE'}) | "
                f"{position.question[:40]} | exit={exit_price:.4f} pnl=${pnl:.4f}")
            self.closed_positions.append(position)
            del self.positions[pos_key]
        else:
            position.shares   -= shares_to_sell
            position.size_usd  = position.shares * position.entry_price
            position.source_shares = current_source_shares
            if pnl > 0:
                compounding_bankroll += pnl * COMPOUNDING_RATE
                logging.info(f"Compounding profit (partial): +${pnl * COMPOUNDING_RATE:.4f} → ${compounding_bankroll:.2f}")
            logging.info(
                f"[{name}] PARTIAL SELL ({'DRY RUN' if self.dry_run else 'LIVE'}) | "
                f"{position.question[:40]} | sold={shares_to_sell:.4f} pnl=${pnl:.4f} remaining={position.shares:.4f}")

    # ------------------------------------------------------------------
    # Per-wallet scan
    # ------------------------------------------------------------------
    async def _scan_wallet(
        self,
        wallet_addr: str,
        config: dict,
    ) -> Tuple[str, Optional[list], set, Dict[str, float]]:
        """
        Fetches (or returns cached) positions for one wallet.
        Returns (wallet_addr, raw_positions, source_token_ids, source_shares_map).
        """
        raw = await self._get_positions(wallet_addr)
        if raw is None:
            return wallet_addr, None, set(), {}

        source_token_ids:   set              = set()
        source_shares_map:  Dict[str, float] = {}
        for pos in raw:
            tid    = pos.get("asset", "")
            shares = float(pos.get("size", pos.get("shares", 0)))
            if tid and shares > 0:
                source_token_ids.add(tid)
                source_shares_map[tid] = shares

        return wallet_addr, raw, source_token_ids, source_shares_map

    # ------------------------------------------------------------------
    # Main scan loop
    # ------------------------------------------------------------------
    async def scan_and_copy(self):
        global current_bankroll, compounding_bankroll, bot_paused_until

        if bot_paused_until and datetime.now() < bot_paused_until:
            remaining = (bot_paused_until - datetime.now()).seconds // 60
            logging.info(f"Bot paused — {remaining} minutes remaining")
            return

        if self.check_drawdown():
            return

        current_bankroll = self.balance.get_balance()
        if current_bankroll is None:
            logging.error("Real pUSD balance unavailable — skipping scan cycle")
            return

        logging.info(
            f"Scanning | balance=${current_bankroll:.2f} | "
            f"available=${self._available_balance():.2f} | "
            f"compounding=${compounding_bankroll:.2f} | "
            f"open={len(self.positions)} | pending={len(self.pending)} | "
            f"seen={len(self.seen._seen)}"
        )

        # ---- Fetch all wallets simultaneously ----
        wallet_items = list(WALLETS.items())
        results: List[Tuple] = await asyncio.gather(
            *[self._scan_wallet(addr, cfg) for addr, cfg in wallet_items],
            return_exceptions=True,
        )

        source_token_ids_by_wallet: Dict[str, set] = {}

        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Wallet scan raised exception: {result}")
                continue

            wallet_addr, raw, source_token_ids, source_shares_map = result
            config = WALLETS[wallet_addr]
            name   = config["name"]

            if raw is None:
                logging.warning(f"Skipping {name} — could not fetch positions")
                continue

            if wallet_addr not in self._first_scan_done:
                self._first_scan_done.add(wallet_addr)
                if config.get("copy_mode") == "new_only":
                    all_keys = {f"{wallet_addr}_{tid}" for tid in source_token_ids}
                    self.seen.snapshot_existing(all_keys)
                    logging.info(
                        f"[{name}] new_only — {len(all_keys)} existing position(s) snapshotted")
                    source_token_ids_by_wallet[wallet_addr] = source_token_ids
                    continue
                else:
                    logging.info(f"[{name}] copy_all — {len(source_token_ids)} position(s) at deployment")

            logging.info(f"[{name}] {len(raw)} position(s) from API, {len(source_token_ids)} active")

            # ---- BUY LOOP ----
            for pos in raw:
                token_id              = pos.get("asset", "")
                market_id             = pos.get("conditionId", "")
                question              = pos.get("title", "Unknown")
                outcome               = pos.get("outcome", "YES")
                size_usd              = float(pos.get("currentValue", 0))
                source_shares_at_copy = float(pos.get("size", pos.get("shares", 0)))

                min_value = 0.0 if config.get("copy_sub_dollar") else 1.0
                if not token_id or size_usd < min_value or size_usd <= 0:
                    continue

                pos_key = f"{wallet_addr}_{token_id}"
                if self.seen.is_seen(pos_key) or pos_key in self.positions or pos_key in self.pending:
                    continue

                if len(self.positions) + len(self.pending) >= MAX_POSITIONS:
                    logging.info("Global max positions reached — skipping new entries")
                    break

                wallet_open    = sum(1 for p in self.positions.values() if p.source_wallet == wallet_addr)
                wallet_pending = sum(1 for p in self.pending.values()   if p.source_wallet == wallet_addr)
                wallet_max     = config.get("max_positions", MAX_POSITIONS)
                if wallet_open + wallet_pending >= wallet_max:
                    logging.info(f"[{name}] wallet cap ({wallet_open}+{wallet_pending}>={wallet_max})")
                    break

                cur_price = market_data.get_current_price(token_id) or float(pos.get("curPrice", 0))
                if cur_price <= 0:
                    logging.info(f"[{name}] SKIP no price | {question[:40]}")
                    continue

                limit_price = round(cur_price, 4)

                if config.get("copy_sub_dollar") and size_usd < 1.0:
                    my_size = round(size_usd, 2)
                else:
                    risk_pct = self.get_risk_percent(limit_price, config)
                    my_size  = round(
                        min(compounding_bankroll * risk_pct, self._available_balance() * 0.95), 2)

                if my_size <= 0 or not self._can_afford(my_size):
                    logging.warning(f"[{name}] Skipping {question[:40]} — cannot afford ${my_size:.2f}")
                    continue

                ok, order_id, actual_price = self.executor.place_limit_buy(token_id, my_size, limit_price)
                if ok:
                    self.seen.mark_seen(pos_key)
                    await market_data.subscribe_tokens([token_id])
                    self.pending[pos_key] = PendingLimitBuy(
                        pos_key=pos_key, token_id=token_id, market_id=market_id,
                        question=question, outcome=outcome,
                        source_wallet=wallet_addr, source_name=name,
                        limit_price=actual_price, size_usd=my_size,
                        order_id=order_id, source_shares=source_shares_at_copy,
                    )
                    logging.info(
                        f"[{name}] LIMIT BUY PLACED | {question[:40]} | "
                        f"${my_size:.2f} @ {actual_price:.4f} "
                        f"(avail=${self._available_balance():.2f} curPrice={cur_price:.4f})")

            source_token_ids_by_wallet[wallet_addr] = source_token_ids

            # Update current prices
            cur_price_map = {
                pos.get("asset", ""): float(pos.get("curPrice", 0))
                for pos in raw if pos.get("asset") and float(pos.get("curPrice", 0)) > 0
            }
            for _pk, _pos in self.positions.items():
                if _pos.source_wallet != wallet_addr:
                    continue
                ws_price = market_data.get_current_price(_pos.token_id)
                if ws_price > 0:
                    _pos.current_price = ws_price
                elif cur_price_map.get(_pos.token_id, 0) > 0:
                    _pos.current_price = cur_price_map[_pos.token_id]

            # ---- SELL LOOP ----
            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr or position.status != "open":
                    continue

                current_source_shares = source_shares_map.get(position.token_id, 0.0)

                if current_source_shares > position.source_shares_at_open:
                    logging.info(
                        f"[{name}] Source ADDED shares "
                        f"{position.source_shares_at_open:.4f} → {current_source_shares:.4f} | "
                        f"{position.question[:40]}")
                    position.source_shares_at_open = current_source_shares
                    position.source_shares         = current_source_shares
                    position.shares_at_open        = position.shares

                if position.token_id not in source_token_ids:
                    logging.info(
                        f"[{name}] Source FULL EXIT — selling {position.shares:.4f} | {position.question[:40]}")
                    await self._execute_sell(position, pos_key, position.shares, name, full_exit=True)

                elif (position.source_shares_at_open > 0 and
                      current_source_shares < position.source_shares * 0.80):
                    hold_ratio         = current_source_shares / position.source_shares_at_open
                    our_target_shares  = round(position.shares_at_open * hold_ratio, 4)
                    our_shares_to_sell = round(position.shares - our_target_shares, 4)
                    if our_shares_to_sell <= 0:
                        continue
                    logging.info(
                        f"[{name}] Source PARTIAL EXIT — {hold_ratio*100:.1f}% | "
                        f"selling {our_shares_to_sell:.4f} of {position.shares:.4f} | {position.question[:40]}")
                    await self._execute_sell(
                        position, pos_key, our_shares_to_sell, name,
                        full_exit=False, current_source_shares=current_source_shares)

        await self._process_pending_orders(source_token_ids_by_wallet)

    # ------------------------------------------------------------------
    # Run loop
    # ------------------------------------------------------------------
    async def run(self):
        logging.info("Bot loop started")
        last_heartbeat = time.time()
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Main loop error: {e}")

            now = time.time()
            if now - last_heartbeat >= 300:
                status = "PAUSED" if bot_paused_until and datetime.now() < bot_paused_until else "ACTIVE"
                wallet_counts: Dict[str, int] = {}
                for p in self.positions.values():
                    wallet_counts[p.source_name] = wallet_counts.get(p.source_name, 0) + 1
                for p in self.pending.values():
                    wallet_counts[p.source_name] = wallet_counts.get(p.source_name, 0) + 1
                slot_summary = " | ".join(f"{n}={c}" for n, c in wallet_counts.items()) or "none"
                logging.info(
                    f"Heartbeat | {status} | balance=${self.balance.cached_balance or 0:.2f} | "
                    f"available=${self._available_balance():.2f} | "
                    f"compounding=${compounding_bankroll:.2f} | "
                    f"slots=[{slot_summary}] total={len(self.positions)+len(self.pending)}/{MAX_POSITIONS} | "
                    f"seen={len(self.seen._seen)} | storage={self.seen.backend}")
                last_heartbeat = now

            # Wake on activity WS event (debounced) OR poll timeout — whichever is first
            market_data.activity_event.clear()
            try:
                await asyncio.wait_for(market_data.activity_event.wait(), timeout=POLL_INTERVAL)
                logging.info("⚡ Activity event — running early scan")
            except asyncio.TimeoutError:
                pass  # Normal 15s poll cycle


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


if __name__ == "__main__":
    asyncio.run(main())
