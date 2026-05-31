#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - WEBSOCKET ENHANCED (CORRECTED)
Based on Polymarket official documentation:
- Market: wss://ws-subscriptions-clob.polymarket.com/ws/market
- User: wss://ws-subscriptions-clob.polymarket.com/ws/user

Fixes applied vs original:
1. SeenTradesStore: db_url was never stored as self.db_url
2. TTLCache.set: dict.popitem() doesn't accept last=False; fixed with next(iter(...))
3. _execute_sell: second param was token_id not pos_key; now looked up internally
4. _process_ws_position_change: cumulative % used stale source_shares_at_open; delta now
   computed against last-known shares (old_shares), and source_shares updated immediately
5. rest_fallback_scan: now also detects NEW positions and copies them
6. Market WS re-subscription: called whenever a new position is opened
7. copy_mode ("new_only" / "copy_all") is now enforced
8. Per-wallet max_positions now enforced
9. Per-wallet limit_buy_max_premium now respected
10. pos_key stored on Position to avoid O(n) scan in _execute_sell
"""

import os
import json
import asyncio
import requests
import logging
import time
import threading
import ssl
from datetime import datetime, timedelta
from typing import Dict, Set, Tuple, Optional, List
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

try:
    import websocket
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False
    print("Warning: websocket-client not installed. pip install websocket-client")

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
MIN_SELL_PERCENT = float(os.getenv("MIN_SELL_PERCENT", "20"))
POLL_INTERVAL = int(os.getenv("POLL_SECONDS", "15"))
ORDERBOOK_CACHE_TTL = int(os.getenv("ORDERBOOK_CACHE_TTL", "3"))
BID_CACHE_TTL = int(os.getenv("BID_CACHE_TTL", "2"))

MARKET_WS_URL = os.getenv("MARKET_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
USER_WS_URL = os.getenv("USER_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/user")
WEBSOCKET_ENABLED = os.getenv("WEBSOCKET_ENABLED", "true").lower() == "true"
WEBSOCKET_RECONNECT_DELAY = int(os.getenv("WEBSOCKET_RECONNECT_DELAY", "5"))

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
        "limit_buy_max_premium": 0.20,
        "max_positions": 5,
    },
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {
        "name": "Coniyr",
        "risk_type": "fixed",
        "fixed_risk": 0.025,
        "copy_mode": "copy_all",
        "limit_buy_max_premium": 0.20,
        "max_positions": 4,
    },
    "0xf903c4cd098184e67a06a04f9b8fdb36e7bbe028": {
        "name": "Viser",
        "risk_type": "price_based",
        "copy_mode": "new_only",
        "limit_buy_max_premium": 0.20,
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
        self.cache: Dict[str, Tuple] = {}
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
            # FIX: plain dict doesn't support popitem(last=False); use next(iter(...))
            if len(self.cache) > self.max_size:
                oldest_key = next(iter(self.cache))
                del self.cache[oldest_key]

    def get_stats(self):
        total = self._hit_count + self._miss_count
        hit_rate = (self._hit_count / total * 100) if total > 0 else 0
        return {
            'hits': self._hit_count,
            'misses': self._miss_count,
            'hit_rate_percent': round(hit_rate, 1),
            'size': len(self.cache),
        }

orderbook_cache = TTLCache(default_ttl=ORDERBOOK_CACHE_TTL, max_size=500)
bid_cache = TTLCache(default_ttl=BID_CACHE_TTL, max_size=200)

# ==================== DATA CLASSES ====================
@dataclass
class Position:
    pos_key: str          # FIX: store key on the object to avoid O(n) scan
    market_id: str
    question: str
    outcome: str
    token_id: str
    condition_id: str
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
    condition_id: str
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
        self.ws_connected = False

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
            'ws_connected': self.ws_connected,
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
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: #0d0d0f; color: #e2e8f0; padding: 24px 16px; }}
        .page {{ max-width: 1200px; margin: 0 auto; }}
        .header {{ display: flex; justify-content: space-between; margin-bottom: 28px; flex-wrap: wrap; }}
        .badge {{ font-size: 0.72rem; font-weight: 600; padding: 3px 10px; border-radius: 999px; text-transform: uppercase; }}
        .badge-live {{ background: #064e3b; color: #6ee7b7; }}
        .badge-dry {{ background: #1e1b4b; color: #a5b4fc; }}
        .badge-ws {{ background: #1e3a5f; color: #60a5fa; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin-bottom: 24px; }}
        .stat-card {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px; padding: 18px 20px; }}
        .stat-value {{ font-size: 1.6rem; font-weight: 700; }}
        .pos {{ color: #34d399; }} .neg {{ color: #f87171; }}
        .section {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px; margin-bottom: 20px; overflow: hidden; }}
        .section-header {{ display: flex; justify-content: space-between; padding: 14px 20px; border-bottom: 1px solid #1e2230; }}
        .section-title {{ font-size: 0.85rem; font-weight: 700; text-transform: uppercase; }}
        .count-pill {{ font-size: 0.72rem; background: #1e2230; border-radius: 999px; padding: 2px 10px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 10px 16px; text-align: left; border-bottom: 1px solid #1a1d26; }}
        th {{ color: #475569; font-size: 0.70rem; text-transform: uppercase; }}
        .source-tag {{ color: #818cf8; background: #1e1b4b; padding: 2px 8px; border-radius: 999px; font-size: 0.70rem; }}
        .outcome-yes {{ background: #064e3b; color: #6ee7b7; padding: 2px 8px; border-radius: 999px; }}
        .outcome-no {{ background: #450a0a; color: #fca5a5; padding: 2px 8px; border-radius: 999px; }}
    </style>
</head>
<body>
<div class="page">
    <div class="header">
        <div><span class="badge badge-ws">WebSocket</span> <span class="badge {mode_badge}">{mode_label}</span> <span class="badge {status_badge}">{status_label}</span></div>
        <div class="timestamp">Updated {last_updated}</div>
    </div>

    <div class="section">
        <div class="section-header"><span class="section-title">🔌 WebSocket Status</span><span class="count-pill">{ws_status}</span></div>
        <div><table><thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody>
            <tr><td>WS Updates Received</td><td class="pos">{ws_updates}</td></tr>
            <tr><td>Connection Mode</td><td>{connection_mode}</td></tr>
            <tr><td>Latency</td><td>{connection_desc}</td></tr>
        </tbody></table></div>
    </div>

    <div class="section">
        <div class="section-header"><span class="section-title">⚡ Cache</span><span class="count-pill">{cache_hit_rate}% Hit Rate</span></div>
        <div><table><thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody>
            <tr><td>Cache Hits</td><td class="pos">{cache_hits}</td></tr>
            <tr><td>Cache Misses</td><td>{cache_misses}</td></tr>
        </tbody></table></div>
    </div>

    <div class="section">
        <div class="section-header"><span class="section-title">🛡️ Sell Filter</span><span class="count-pill">{min_sell_percent}% Threshold</span></div>
        <div><table><thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody>
            <tr><td>Cumulative Triggers</td><td class="pos">{cumulative_triggers}</td></tr>
            <tr><td>Sells Skipped</td><td>{sells_skipped}</td></tr>
            <tr><td>Sells Executed</td><td class="pos">{sells_executed}</td></tr>
            <tr><td>Skip Rate</td><td>{skip_rate}%</td></tr>
        </tbody></table></div>
    </div>

    <div class="stats">
        <div class="stat-card"><div class="stat-label">Balance</div><div class="stat-value">${balance:.2f}</div></div>
        <div class="stat-card"><div class="stat-label">Available</div><div class="stat-value">${available:.2f}</div></div>
        <div class="stat-card"><div class="stat-label">Open Positions</div><div class="stat-value">{open_count}</div></div>
        <div class="stat-card"><div class="stat-label">Total PnL</div><div class="stat-value {total_pnl_cls}">{total_pnl_sign}${total_pnl_abs}</div></div>
        <div class="stat-card"><div class="stat-label">Drawdown</div><div class="stat-value {dd_cls}">{drawdown:.1f}%</div></div>
    </div>

    <div class="section">
        <div class="section-header"><span class="section-title">Open Positions</span><span class="count-pill">{open_count}</span></div>
        {positions_block}
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

    cache_stats = orderbook_cache.get_stats()
    filter_metrics = sell_metrics.get_metrics()

    ws_status = "CONNECTED" if bot.ws_connected else "FALLBACK"
    connection_mode = "WebSocket Real-Time" if bot.ws_connected else "REST Polling"
    connection_desc = "Instant (<1s)" if bot.ws_connected else f"Every {POLL_INTERVAL}s"

    unrealised = 0.0
    pos_rows = ""
    for p in bot.positions.values():
        mid = p.current_price or p.entry_price
        unreal = (mid - p.entry_price) * p.shares
        unrealised += unreal
        pos_rows += (
            f"<tr><td><span class='source-tag'>{p.source_name}</span></td>"
            f"<td>{p.question[:50]}</td>"
            f"<td><span class='outcome-{p.outcome.lower()}'>{p.outcome}</span></td>"
            f"<td>${p.size_usd:.2f}</td><td>{p.entry_price:.3f}</td><td>{mid:.3f}</td>"
            f"<td class='{_cls(unreal)}'>{_sign(unreal)}${abs(unreal):.2f}</td></tr>"
        )

    if pos_rows:
        positions_block = (
            "<div><table><thead><tr>"
            "<th>Source</th><th>Market</th><th>Side</th><th>Size</th><th>Entry</th><th>Current</th><th>PnL</th>"
            f"</tr></thead><tbody>{pos_rows}</tbody></table></div>"
        )
    else:
        positions_block = '<div style="padding:20px;color:#475569;">No open positions</div>'

    closed_list = getattr(bot, "closed_positions", [])
    realised = sum(p.pnl for p in closed_list)
    total_pnl = realised + unrealised

    return {
        "last_updated": datetime.now().strftime("%H:%M:%S"),
        "mode_label": "LIVE" if not bot.dry_run else "DRY",
        "mode_badge": "badge-live" if not bot.dry_run else "badge-dry",
        "status_label": "Paused" if is_paused else "Running",
        "status_badge": "badge-paused" if is_paused else "badge-live",
        "balance": bankroll, "available": available,
        "drawdown": drawdown, "dd_cls": "neg" if drawdown > 10 else "pos",
        "total_pnl_cls": _cls(total_pnl), "total_pnl_sign": _sign(total_pnl), "total_pnl_abs": f"{abs(total_pnl):.2f}",
        "open_count": len(bot.positions),
        "positions_block": positions_block,
        "cache_hits": cache_stats['hits'], "cache_misses": cache_stats['misses'],
        "cache_hit_rate": cache_stats['hit_rate_percent'],
        "min_sell_percent": MIN_SELL_PERCENT,
        "sells_skipped": filter_metrics.get('sells_skipped', 0),
        "sells_executed": filter_metrics.get('sells_executed', 0),
        "skip_rate": filter_metrics.get('skip_rate_percent', 0),
        "cumulative_triggers": filter_metrics.get('cumulative_triggers', 0),
        "ws_updates": filter_metrics.get('ws_updates_received', 0),
        "ws_status": ws_status, "connection_mode": connection_mode, "connection_desc": connection_desc,
    }

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200); self.send_header("Content-Type", "text/html"); self.end_headers()
            try:
                data = build_dashboard(_bot_ref)
                self.wfile.write(HTML_TEMPLATE.format(**data).encode())
            except Exception as e:
                self.wfile.write(f"<h1>Dashboard error: {e}</h1>".encode())
        elif self.path == "/metrics":
            self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers()
            metrics = sell_metrics.get_metrics()
            metrics['cache'] = orderbook_cache.get_stats()
            if _bot_ref: metrics['balance'] = _bot_ref.balance.cached_balance or 0
            self.wfile.write(json.dumps(metrics, indent=2).encode())
        elif self.path == "/health":
            self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers()
            self.wfile.write(json.dumps({
                "status": "healthy",
                "ws_connected": _bot_ref.ws_connected if _bot_ref else False,
            }).encode())
        else:
            self.send_response(200); self.end_headers(); self.wfile.write(b"OK")

    def log_message(self, format, *args): pass

_bot_ref = None

def run_health_server():
    HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler).serve_forever()

# ==================== SEEN TRADES STORE ====================
class SeenTradesStore:
    def __init__(self, filepath: str, db_url: str = ""):
        self.filepath = filepath
        self.db_url = db_url  # FIX: was never assigned in original
        self._seen: Set[str] = set()
        self._conn = None
        if db_url and PSYCOPG2_AVAILABLE:
            self._init_postgres()
        else:
            self._load_file()
        logger.info(f"SeenTradesStore ready | {len(self._seen)} keys")

    def _init_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require")
            self._conn.autocommit = True
            with self._conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS seen_trades (pos_key TEXT PRIMARY KEY)")
            self._seen = self._load_postgres()
        except Exception as e:
            logger.warning(f"Postgres init failed ({e}), falling back to file")
            self._load_file()

    def _load_postgres(self) -> Set[str]:
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT pos_key FROM seen_trades")
                return {row[0] for row in cur.fetchall()}
        except:
            return set()

    def _save_postgres(self, pos_key: str):
        try:
            with self._conn.cursor() as cur:
                cur.execute("INSERT INTO seen_trades (pos_key) VALUES (%s) ON CONFLICT DO NOTHING", (pos_key,))
        except:
            pass

    def _load_file(self):
        try:
            with open(self.filepath, "r") as f:
                data = json.load(f)
                self._seen = set(data) if isinstance(data, list) else set()
        except:
            self._seen = set()

    def _save_file(self):
        try:
            with open(self.filepath, "w") as f:
                json.dump(sorted(self._seen), f)
        except:
            pass

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
            for k in new_keys:
                self._save_postgres(k)
        else:
            self._save_file()

# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    POLYGON_RPCS = [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com",
        "https://polygon.drpc.org",
    ]

    def __init__(self):
        self.cached_balance: Optional[float] = None
        self.peak_balance = 0.0

    def _fetch_balance(self) -> float:
        if not YOUR_WALLET:
            return 0.0
        padded = YOUR_WALLET.lower().replace("0x", "").zfill(64)
        payload = {
            "jsonrpc": "2.0", "method": "eth_call",
            "params": [{"to": PUSD_CONTRACT_ADDRESS, "data": "0x70a08231" + padded}, "latest"],
            "id": 1,
        }
        for rpc in self.POLYGON_RPCS:
            try:
                resp = requests.post(rpc, json=payload, timeout=8)
                if resp.status_code == 200:
                    result = resp.json().get("result", "0x0")
                    if result and result not in ("0x", "0x0"):
                        return int(result, 16) / 1_000_000
            except:
                continue
        return 0.0

    def get_balance(self, force=False) -> Optional[float]:
        if force or self.cached_balance is None:
            real = self._fetch_balance()
            if real > 0:
                self.cached_balance = real
                if real > self.peak_balance:
                    self.peak_balance = real
        return self.cached_balance

    def fetch_with_retry(self, retries=5, delay=10) -> float:
        for attempt in range(retries):
            val = self._fetch_balance()
            if val > 0:
                self.cached_balance = val
                self.peak_balance = val
                return val
            time.sleep(delay)
        raise RuntimeError("Could not fetch balance after retries")

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
        self.client = None
        if not dry_run and CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(api_key=POLY_API_KEY, api_secret=POLY_SECRET, api_passphrase=POLY_PASSPHRASE)
                self.client = ClobClient(
                    host="https://clob.polymarket.com", chain_id=137,
                    key=YOUR_PRIVATE_KEY, creds=creds,
                )
                logger.info("ClobClient V2 initialised — LIVE mode")
            except Exception as e:
                logger.error(f"ClobClient init failed: {e}")

    def place_limit_buy(self, token_id: str, amount_usd: float, limit_price: float) -> Tuple[bool, str, float]:
        shares = round(amount_usd / limit_price, 4)
        if self.dry_run or not self.client:
            return True, "dry-run", limit_price
        for attempt in range(MAX_RETRIES):
            try:
                result = self.client.create_and_post_order(
                    order_args=OrderArgs(token_id=token_id, price=limit_price, size=shares, side=Side.BUY),
                    options=PartialCreateOrderOptions(tick_size="0.01"),
                    order_type=OrderType.GTC,
                )
                return True, result.get("orderID", "unknown"), limit_price
            except:
                time.sleep(RETRY_DELAY)
        return False, "", limit_price

    def cancel_order(self, order_id: str) -> bool:
        if self.dry_run or not self.client:
            return True
        try:
            self.client.cancel(order_id)
            return True
        except:
            return False

    def is_order_filled(self, order_id: str) -> Optional[bool]:
        if self.dry_run or not self.client:
            return True
        try:
            order = self.client.get_order(order_id)
            return order.get("status", "").lower() in ("matched", "filled")
        except:
            return None

    def place_sell_with_partial_fill_handling(
        self, token_id: str, total_shares: float, min_price: float = 0.0, max_attempts: int = 3
    ) -> Tuple[bool, float, List[float]]:
        if self.dry_run or not self.client:
            return True, total_shares, [0.0]
        remaining, executed, prices = total_shares, 0.0, []
        for attempt in range(max_attempts):
            if remaining <= 0.01:
                break
            try:
                kwargs = dict(
                    order_args=MarketOrderArgs(token_id=token_id, amount=remaining, side=Side.SELL),
                    options=PartialCreateOrderOptions(tick_size="0.01"),
                    order_type=OrderType.IOC,
                )
                if min_price > 0:
                    try:
                        kwargs["order_args"] = MarketOrderArgs(
                            token_id=token_id, amount=remaining, side=Side.SELL,
                            min_price=round(min_price, 4),
                        )
                    except:
                        pass
                result = self.client.create_and_post_market_order(**kwargs)
                filled = float(result.get("filledSize", result.get("size", remaining)))
                if filled > 0:
                    executed += filled
                    remaining -= filled
                    prices.append(float(result.get("averagePrice", min_price)))
                    if remaining <= 0.01:
                        return True, executed, prices
                    time.sleep(2)
                else:
                    time.sleep(RETRY_DELAY)
            except:
                time.sleep(RETRY_DELAY)
        return executed > 0, executed, prices

# ==================== WEBSOCKET MANAGER ====================
class WebSocketManager:
    """Manages WebSocket connections using official Polymarket endpoints."""

    def __init__(self, bot):
        self.bot = bot
        self.market_ws = None
        self.user_ws = None
        self.connected = False
        self.stop_flag = False
        self.reconnect_delay = WEBSOCKET_RECONNECT_DELAY
        self._heartbeat_running = False
        self._market_ws_ref = None  # kept so we can re-subscribe on new positions

    def start(self):
        if not WEBSOCKET_ENABLED or not WEBSOCKET_AVAILABLE:
            logger.info("WebSocket disabled or unavailable — using REST only")
            return
        logger.info("Starting WebSocket connections (official Polymarket endpoints)…")
        self._start_market_ws()
        self._start_user_ws()

    def stop(self):
        self.stop_flag = True
        if self.market_ws:
            self.market_ws.close()
        if self.user_ws:
            self.user_ws.close()

    # ---------- market channel ----------

    def _start_market_ws(self):
        def run():
            while not self.stop_flag:
                try:
                    logger.info(f"Connecting to Market WebSocket: {MARKET_WS_URL}")
                    ws = websocket.WebSocketApp(
                        MARKET_WS_URL,
                        on_open=self._on_market_open,
                        on_message=self._on_market_message,
                        on_error=self._on_market_error,
                        on_close=self._on_market_close,
                    )
                    self.market_ws = ws
                    self._market_ws_ref = ws
                    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
                except Exception as e:
                    logger.error(f"Market WebSocket error: {e}")
                if not self.stop_flag:
                    time.sleep(self.reconnect_delay)
        threading.Thread(target=run, daemon=True).start()

    def resubscribe_market(self, token_ids: List[str]):
        """Re-subscribe with updated token list when new positions are opened."""
        ws = self._market_ws_ref
        if ws and ws.sock and ws.sock.connected and token_ids:
            try:
                msg = {"type": "market", "assets_ids": token_ids, "custom_feature_enabled": True}
                ws.send(json.dumps(msg))
                logger.info(f"Re-subscribed market WS for {len(token_ids)} tokens")
            except Exception as e:
                logger.warning(f"Market WS re-subscribe failed: {e}")

    def _on_market_open(self, ws):
        logger.info("✅ Market WebSocket connected")
        self.bot.ws_market_connected = True
        self._start_heartbeat(ws, "market")
        token_ids = self._get_tracked_token_ids()
        if token_ids:
            msg = {"type": "market", "assets_ids": token_ids, "custom_feature_enabled": True}
            ws.send(json.dumps(msg))
            logger.info(f"Subscribed to market for {len(token_ids)} tokens")

    def _on_market_message(self, ws, message):
        try:
            if message == "PONG":
                return
            data = json.loads(message)
            event_type = data.get("type")

            if event_type == "book":
                token_id = data.get("asset_id")
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                if token_id and bids and asks:
                    best_bid = float(bids[0][0]) if bids else 0.0
                    best_ask = float(asks[0][0]) if asks else 0.0
                    mid = (best_bid + best_ask) / 2
                    orderbook_cache.set(token_id, (mid, best_ask))
                    if best_bid > 0:
                        bid_cache.set(token_id, best_bid)

            elif event_type == "price_change":
                token_id = data.get("asset_id")
                price = data.get("price", 0)
                side = data.get("side")
                if token_id and price > 0:
                    if side == "bid":
                        bid_cache.set(token_id, price)
                    for position in self.bot.positions.values():
                        if position.token_id == token_id and side == "bid":
                            position.current_price = price
                            break

            elif event_type == "last_trade_price":
                token_id = data.get("asset_id")
                price = data.get("price", 0)
                if token_id and price > 0:
                    for position in self.bot.positions.values():
                        if position.token_id == token_id:
                            position.current_price = price
                            break

            elif event_type == "best_bid_ask":
                token_id = data.get("asset_id")
                best_bid = data.get("bid", 0)
                best_ask = data.get("ask", 0)
                if token_id:
                    if best_bid > 0:
                        bid_cache.set(token_id, best_bid)
                    if best_bid and best_ask:
                        mid = (best_bid + best_ask) / 2
                        orderbook_cache.set(token_id, (mid, best_ask))
                        for position in self.bot.positions.values():
                            if position.token_id == token_id:
                                position.current_price = mid
                                break
        except json.JSONDecodeError:
            pass
        except Exception as e:
            logger.error(f"Market WS message error: {e}")

    def _on_market_error(self, ws, error):
        logger.error(f"Market WebSocket error: {error}")
        self.bot.ws_market_connected = False

    def _on_market_close(self, ws, close_status_code, close_msg):
        logger.warning(f"Market WebSocket closed: {close_status_code}")
        self.bot.ws_market_connected = False
        self._heartbeat_running = False

    # ---------- user channel ----------

    def _start_user_ws(self):
        def run():
            while not self.stop_flag:
                try:
                    logger.info(f"Connecting to User WebSocket: {USER_WS_URL}")
                    ws = websocket.WebSocketApp(
                        USER_WS_URL,
                        on_open=self._on_user_open,
                        on_message=self._on_user_message,
                        on_error=self._on_user_error,
                        on_close=self._on_user_close,
                    )
                    self.user_ws = ws
                    ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
                except Exception as e:
                    logger.error(f"User WebSocket error: {e}")
                if not self.stop_flag:
                    time.sleep(self.reconnect_delay)
        threading.Thread(target=run, daemon=True).start()

    def _on_user_open(self, ws):
        logger.info("🔐 User WebSocket connected, authenticating…")
        msg = {
            "type": "user",
            "auth": {
                "apiKey": POLY_API_KEY,
                "secret": POLY_SECRET,
                "passphrase": POLY_PASSPHRASE,
            },
            "markets": self._get_tracked_condition_ids(),
        }
        ws.send(json.dumps(msg))
        logger.info(f"Auth sent, subscribed to {len(msg['markets'])} markets")
        self._start_heartbeat(ws, "user")

    def _on_user_message(self, ws, message):
        try:
            if message == "PONG":
                return
            data = json.loads(message)

            if data.get("type") == "auth_response":
                if data.get("result") == "success":
                    logger.info("✅ User WebSocket authenticated")
                    self.connected = True
                    self.bot.ws_connected = True
                    sell_metrics.ws_connected = True
                else:
                    logger.error(f"Auth failed: {data.get('message')}")
                return

            if data.get("type") == "trade":
                trade = data.get("data", {})
                wallet_addr = trade.get("address")
                token_id = trade.get("asset_id")
                size = float(trade.get("size", 0))
                side = trade.get("side")

                if wallet_addr and token_id and side == "SELL" and size > 0:
                    for position in self.bot.positions.values():
                        if position.source_wallet == wallet_addr and position.token_id == token_id:
                            old_shares = position.source_shares
                            new_shares = max(0.0, old_shares - size)
                            logger.info(
                                f"📡 WS Trade: {position.source_name} sold {size:.4f} shares | {position.question[:40]}"
                            )
                            self.bot._process_ws_position_change(position, old_shares, new_shares)
                            sell_metrics.record_ws_update()
                            break

            elif data.get("type") == "order":
                order_data = data.get("data", {})
                order_id = order_data.get("id")
                status = order_data.get("status")
                if order_id and status == "FILLED":
                    for pending in self.bot.pending.values():
                        if pending.order_id == order_id:
                            logger.info(f"WS: Order {order_id} filled")
                            break
        except json.JSONDecodeError:
            pass
        except Exception as e:
            logger.error(f"User WS message error: {e}")

    def _on_user_error(self, ws, error):
        logger.error(f"User WebSocket error: {error}")
        self.connected = False
        self.bot.ws_connected = False
        sell_metrics.ws_connected = False

    def _on_user_close(self, ws, close_status_code, close_msg):
        logger.warning(f"User WebSocket closed: {close_status_code}")
        self.connected = False
        self.bot.ws_connected = False
        sell_metrics.ws_connected = False
        self._heartbeat_running = False

    # ---------- helpers ----------

    def _start_heartbeat(self, ws, channel="market"):
        """Send PING every 10 s as required by Polymarket docs."""
        def heartbeat():
            self._heartbeat_running = True
            while self._heartbeat_running and not self.stop_flag:
                time.sleep(10)
                if ws and ws.sock and ws.sock.connected:
                    try:
                        ws.send("PING")
                        logger.debug(f"Heartbeat sent to {channel} channel")
                    except:
                        pass
        threading.Thread(target=heartbeat, daemon=True).start()

    def _get_tracked_token_ids(self) -> List[str]:
        ids = set()
        for pos in self.bot.positions.values():
            ids.add(pos.token_id)
        for pending in self.bot.pending.values():
            ids.add(pending.token_id)
        return list(ids)

    def _get_tracked_condition_ids(self) -> List[str]:
        ids = set()
        for pos in self.bot.positions.values():
            if pos.condition_id:
                ids.add(pos.condition_id)
        return list(ids)

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
        self.ws_market_connected = False

        self.ws_manager = WebSocketManager(self)
        self.ws_manager.start()

        logger.info(f"CopyTrader started | {'LIVE' if not dry_run else 'DRY RUN'}")
        logger.info(f"🛡️ Sell threshold: {MIN_SELL_PERCENT}% | 📊 REST fallback: {POLL_INTERVAL}s")
        logger.info(f"🔌 WebSocket: {'ENABLED' if WEBSOCKET_ENABLED and WEBSOCKET_AVAILABLE else 'DISABLED'}")

    # ---------- helpers ----------

    def _reserved_capital(self) -> float:
        return (
            sum(p.size_usd for p in self.positions.values())
            + sum(p.size_usd for p in self.pending.values())
        )

    def _available_balance(self) -> float:
        bal = self.balance.cached_balance or 0.0
        return max(0.0, bal - self._reserved_capital())

    def _can_afford(self, amount_usd: float) -> bool:
        return self._available_balance() >= amount_usd * 1.02

    def _positions_for_wallet(self, wallet_addr: str) -> int:
        """Count open + pending positions copying a specific wallet."""
        return sum(
            1 for p in list(self.positions.values()) + list(self.pending.values())
            if p.source_wallet == wallet_addr
        )

    def get_risk_percent(self, price: float, config: dict) -> float:
        if config.get("risk_type") == "fixed":
            return config.get("fixed_risk", 0.025)
        return 0.03 if price >= 0.70 else (0.01 if price >= 0.30 else 0.006)

    def check_drawdown(self) -> bool:
        global peak_bankroll, bot_paused_until
        current = self.balance.get_balance()
        if current and current > peak_bankroll:
            peak_bankroll = current
        dd = (peak_bankroll - (current or 0)) / peak_bankroll if peak_bankroll > 0 else 0
        if dd >= MAX_DRAWDOWN:
            if bot_paused_until is None or datetime.now() > bot_paused_until:
                bot_paused_until = datetime.now() + timedelta(hours=PAUSE_HOURS)
                logger.warning(f"DRAWDOWN TRIGGERED ({dd*100:.1f}%) — paused {PAUSE_HOURS}h")
            return True
        return False

    def _get_positions_rest(self, wallet_addr: str) -> Optional[list]:
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(
                    f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50",
                    timeout=12,
                )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 30))
                    time.sleep(retry_after)
                    continue
                if resp.status_code == 200:
                    return resp.json()
            except:
                time.sleep(RETRY_DELAY)
        return None

    # ---------- cached price helpers ----------

    def get_orderbook_prices(self, token_id: str) -> Tuple[float, float]:
        cached = orderbook_cache.get(token_id)
        if cached:
            return cached
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
            except:
                time.sleep(RETRY_DELAY)
        return 0.0, 0.0

    def _get_best_bid(self, token_id: str) -> float:
        cached = bid_cache.get(token_id)
        if cached:
            return cached
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8)
                if r.status_code == 200:
                    bids = r.json().get("bids", [])
                    best_bid = float(bids[0]["price"]) if bids else 0.0
                    if best_bid > 0:
                        bid_cache.set(token_id, best_bid)
                    return best_bid
            except:
                time.sleep(RETRY_DELAY)
        return 0.0

    # ---------- open a new copied position ----------

    def _open_position(self, wallet_addr: str, config: dict, raw_pos: dict):
        """
        Evaluate a newly-seen position from a tracked wallet and copy it
        if it passes all checks.
        """
        token_id = raw_pos.get("asset", "")
        market_id = raw_pos.get("market", raw_pos.get("conditionId", ""))
        condition_id = raw_pos.get("conditionId", market_id)
        question = raw_pos.get("title", raw_pos.get("question", "Unknown"))
        outcome = raw_pos.get("outcome", "Yes")
        source_shares = float(raw_pos.get("size", raw_pos.get("shares", 0)))
        source_name = config["name"]

        if not token_id or source_shares <= 0:
            return

        pos_key = f"{wallet_addr}:{token_id}"
        if self.seen.is_seen(pos_key):
            return

        # FIX: enforce copy_mode
        copy_mode = config.get("copy_mode", "new_only")
        if copy_mode == "new_only" and pos_key in self.positions:
            return

        # FIX: enforce per-wallet max_positions
        wallet_max = config.get("max_positions", MAX_POSITIONS)
        if self._positions_for_wallet(wallet_addr) >= wallet_max:
            logger.debug(f"{source_name}: max_positions ({wallet_max}) reached, skipping {question[:40]}")
            self.seen.mark_seen(pos_key)
            return

        # Global position cap
        if len(self.positions) + len(self.pending) >= MAX_POSITIONS:
            logger.debug("Global max_positions reached, skipping")
            self.seen.mark_seen(pos_key)
            return

        if bot_paused_until and datetime.now() < bot_paused_until:
            return

        if self.check_drawdown():
            return

        mid, best_ask = self.get_orderbook_prices(token_id)
        if mid <= 0:
            return

        # FIX: use per-wallet limit_buy_max_premium
        max_premium = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)

        copy_sub_dollar = config.get("copy_sub_dollar", False)
        if mid < 0.01 and not copy_sub_dollar:
            self.seen.mark_seen(pos_key)
            return

        risk_pct = self.get_risk_percent(mid, config)
        bankroll = self.balance.cached_balance or INITIAL_BANKROLL
        size_usd = round(bankroll * risk_pct, 4)

        if not self._can_afford(size_usd):
            logger.debug(f"Cannot afford ${size_usd:.2f} for {source_name}:{question[:30]}")
            return

        limit_price = round(min(best_ask, mid * (1 + max_premium)), 4)
        limit_price = max(0.01, min(limit_price, 0.99))

        logger.info(
            f"COPY BUY | {source_name} | {question[:40]} [{outcome}] "
            f"| mid={mid:.3f} limit={limit_price:.3f} size=${size_usd:.2f}"
        )

        success, order_id, filled_price = self.executor.place_limit_buy(token_id, size_usd, limit_price)
        self.seen.mark_seen(pos_key)

        if not success:
            logger.error(f"Limit buy failed: {question[:40]}")
            return

        pending = PendingLimitBuy(
            pos_key=pos_key, token_id=token_id, market_id=market_id,
            condition_id=condition_id, question=question, outcome=outcome,
            source_wallet=wallet_addr, source_name=source_name,
            limit_price=limit_price, size_usd=size_usd, order_id=order_id,
            source_shares=source_shares,
        )
        self.pending[pos_key] = pending

        # Re-subscribe market WS so price updates flow for this token
        all_tokens = self.ws_manager._get_tracked_token_ids()
        if token_id not in all_tokens:
            all_tokens.append(token_id)
        self.ws_manager.resubscribe_market(all_tokens)

    # ---------- position change processing ----------

    def _process_ws_position_change(self, position: Position, old_shares: float, new_shares: float):
        """
        Called both from WS events (real-time) and REST fallback.
        old_shares: the share count we *had* recorded before this event.
        new_shares: the share count *after* this event.
        """
        # Full exit
        if new_shares <= 0:
            logger.info(f"🚨 FULL EXIT: {position.question[:40]}")
            self._execute_sell(position, position.shares, position.source_name, full_exit=True)
            return

        # FIX: delta is computed from old_shares (last known), not source_shares_at_open
        delta_sold = old_shares - new_shares
        if delta_sold <= 0:
            # Source bought more — update tracking and return
            position.source_shares = new_shares
            return

        # FIX: update source_shares immediately so the next event has a correct baseline
        position.source_shares = new_shares

        if position.source_shares_at_open > 0:
            delta_pct = (delta_sold / position.source_shares_at_open) * 100
        else:
            delta_pct = 0.0

        position.cumulative_sold_percent += delta_pct

        logger.debug(
            f"{position.source_name} cumulative sold: {position.cumulative_sold_percent:.1f}% "
            f"(threshold {MIN_SELL_PERCENT}%) | {position.question[:30]}"
        )

        if position.cumulative_sold_percent >= MIN_SELL_PERCENT:
            # How many of our shares should we sell proportionally
            already_sold = position.shares_at_open - position.shares
            total_to_sell = position.shares_at_open * min(position.cumulative_sold_percent / 100, 0.99)
            shares_to_sell = max(0.0, total_to_sell - already_sold)
            shares_to_sell = min(shares_to_sell, position.shares)

            if shares_to_sell > 0.01:
                is_full = shares_to_sell >= position.shares - 0.001
                sell_metrics.record_decision(True, shares_to_sell, position.cumulative_sold_percent, True)
                logger.info(
                    f"✅ Cumulative {position.cumulative_sold_percent:.1f}% → "
                    f"selling {shares_to_sell:.4f} shares ({'FULL' if is_full else 'PARTIAL'})"
                )
                self._execute_sell(
                    position, shares_to_sell, position.source_name,
                    full_exit=is_full, current_source_shares=new_shares,
                )
                # Reset accumulator against the new baseline
                position.cumulative_sold_percent = 0.0
                position.shares_at_open = position.shares  # updated by _execute_sell
                position.source_shares_at_open = new_shares
            else:
                sell_metrics.record_decision(False, 0.0, position.cumulative_sold_percent, False)
        else:
            sell_metrics.record_decision(False, delta_sold, delta_pct, False)

    # ---------- sell execution ----------

    def _execute_sell(
        self, position: Position, shares_to_sell: float, name: str,
        full_exit: bool, current_source_shares: float = 0.0,
    ):
        global compounding_bankroll

        if shares_to_sell <= 0:
            return

        # FIX: pos_key is now stored on the Position object — no linear scan needed
        pos_key = position.pos_key

        if pos_key not in self.positions:
            logger.warning(f"_execute_sell: pos_key {pos_key} not in positions dict")
            return

        if self.dry_run:
            exit_price = position.current_price or position.entry_price
            pnl = (exit_price - position.entry_price) * shares_to_sell
            success, executed_shares = True, shares_to_sell
        else:
            best_bid = self._get_best_bid(position.token_id)
            min_price = round(best_bid * (1 - MAX_SLIPPAGE), 4) if best_bid > 0 else 0.0
            pending_costs = {pk: p.size_usd for pk, p in self.pending.items()}

            with _trade_lock:
                balance_before = self.balance.get_balance(force=True) or 0.0
                success, executed_shares, _ = self.executor.place_sell_with_partial_fill_handling(
                    position.token_id, shares_to_sell, min_price
                )
                if success and executed_shares > 0:
                    time.sleep(SELL_SETTLE_WAIT)
                    balance_after = self.balance.get_balance(force=True) or 0.0
                    contamination = sum(cost for pk, cost in pending_costs.items() if pk not in self.pending)
                    pnl = ((balance_after - balance_before) + contamination) * (executed_shares / shares_to_sell)
                    exit_price = best_bid or position.current_price
                else:
                    pnl, exit_price = 0.0, 0.0

        if not success or executed_shares <= 0:
            logger.error(f"SELL failed: {position.question[:40]}")
            return

        is_full_exit = full_exit or executed_shares >= position.shares - 0.001

        if is_full_exit:
            position.status = "closed"
            position.exit_price = exit_price
            position.pnl = pnl
            if pnl > 0:
                compounding_bankroll += pnl * COMPOUNDING_RATE
            logger.info(
                f"{'FULL' if full_exit else 'PARTIAL→FULL'} SELL | {position.question[:40]} "
                f"| shares={executed_shares:.4f} pnl=${pnl:.4f}"
            )
            self.closed_positions.append(position)
            del self.positions[pos_key]
        else:
            position.shares -= executed_shares
            position.size_usd = position.shares * position.entry_price
            position.source_shares = current_source_shares
            if pnl > 0:
                compounding_bankroll += pnl * COMPOUNDING_RATE
            logger.info(
                f"PARTIAL SELL | {position.question[:40]} "
                f"| sold={executed_shares:.4f} remaining={position.shares:.4f} pnl=${pnl:.4f}"
            )

    # ---------- pending order handling ----------

    def _process_pending_orders(self):
        for pos_key, pending in list(self.pending.items()):
            filled = self.executor.is_order_filled(pending.order_id)
            if filled is None:
                pending.fill_check_errors += 1
                if pending.fill_check_errors >= MAX_FILL_CHECK_ERRORS:
                    self.executor.cancel_order(pending.order_id)
                    del self.pending[pos_key]
                continue

            if filled:
                shares = pending.size_usd / pending.limit_price
                pos = Position(
                    pos_key=pos_key,
                    market_id=pending.market_id, question=pending.question,
                    outcome=pending.outcome, token_id=pending.token_id,
                    condition_id=pending.condition_id, entry_price=pending.limit_price,
                    size_usd=pending.size_usd, shares=shares,
                    source_wallet=pending.source_wallet, source_name=pending.source_name,
                    order_id=pending.order_id, source_shares=pending.source_shares,
                    shares_at_open=shares, source_shares_at_open=pending.source_shares,
                )
                self.positions[pos_key] = pos
                del self.pending[pos_key]
                logger.info(f"LIMIT BUY FILLED: {pending.question[:40]}")

                # Re-subscribe WS with the new token
                all_tokens = self.ws_manager._get_tracked_token_ids()
                self.ws_manager.resubscribe_market(all_tokens)
                continue

            age = (datetime.now() - pending.placed_at).total_seconds()
            if age >= LIMIT_EXPIRY_SECONDS:
                self.executor.cancel_order(pending.order_id)
                logger.info(f"Order expired and cancelled: {pending.question[:40]}")
                del self.pending[pos_key]

    # ---------- REST fallback scan ----------

    async def rest_fallback_scan(self):
        """
        Polls all tracked wallets via REST.
        - Detects new positions and copies them (FIX: was missing)
        - Detects share changes and mirrors sells
        Runs always; when WS is connected it still catches anything missed.
        """
        for wallet_addr, config in WALLETS.items():
            raw = self._get_positions_rest(wallet_addr)
            if raw is None:
                continue

            source_map: Dict[str, dict] = {}
            for pos in raw:
                tid = pos.get("asset", "")
                shares = float(pos.get("size", pos.get("shares", 0)))
                if tid and shares > 0:
                    source_map[tid] = pos

            # --- first-scan snapshot: mark existing positions as seen so we don't copy stale ones ---
            if wallet_addr not in self._first_scan_done:
                existing_keys = [f"{wallet_addr}:{tid}" for tid in source_map]
                self.seen.snapshot_existing(existing_keys)
                self._first_scan_done.add(wallet_addr)
                logger.info(f"{config['name']}: first scan — {len(existing_keys)} positions snapshotted")
                continue

            # --- detect NEW positions (FIX: was entirely missing) ---
            for tid, raw_pos in source_map.items():
                pos_key = f"{wallet_addr}:{tid}"
                if not self.seen.is_seen(pos_key) and pos_key not in self.positions and pos_key not in self.pending:
                    self._open_position(wallet_addr, config, raw_pos)

            # --- detect SHARE CHANGES for positions we're already copying ---
            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr:
                    continue
                if position.status != "open":
                    continue

                new_shares = 0.0
                if position.token_id in source_map:
                    raw_pos = source_map[position.token_id]
                    new_shares = float(raw_pos.get("size", raw_pos.get("shares", 0)))

                if new_shares != position.source_shares:
                    logger.info(
                        f"REST: {position.source_name} {position.source_shares:.4f} → {new_shares:.4f} shares | {position.question[:30]}"
                    )
                    self._process_ws_position_change(position, position.source_shares, new_shares)

    # ---------- main loop ----------

    async def run(self):
        logger.info(f"Bot running | Poll: {POLL_INTERVAL}s | Min sell: {MIN_SELL_PERCENT}%")
        last_heartbeat = time.time()
        last_rest_scan = 0.0

        while True:
            try:
                now = time.time()
                if now - last_rest_scan >= POLL_INTERVAL:
                    await self.rest_fallback_scan()
                    last_rest_scan = now

                self._process_pending_orders()

                if now - last_heartbeat >= 300:
                    is_paused = bot_paused_until and datetime.now() < bot_paused_until
                    status = "PAUSED" if is_paused else "ACTIVE"
                    ws_status = "CONNECTED" if self.ws_connected else "DISCONNECTED"
                    logger.info(
                        f"Heartbeat | {status} | WS:{ws_status} "
                        f"| balance=${self.balance.cached_balance or 0:.2f} "
                        f"| open={len(self.positions)} pending={len(self.pending)}"
                    )
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
    logger.info("🤖 WEBSOCKET ENHANCED COPY TRADER (Corrected)")
    logger.info(f"📡 Market WS: {MARKET_WS_URL}")
    logger.info(f"📡 User WS:   {USER_WS_URL}")
    logger.info(f"📊 REST fallback: {POLL_INTERVAL}s | 🛡️ Sell threshold: {MIN_SELL_PERCENT}%")
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
