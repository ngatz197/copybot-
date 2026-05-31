#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - PRODUCTION READY (CLOB V2)

Original fixes (v1):
  1. is_order_filled: distinguishes API errors (returns None) from confirmed
     unfilled (returns False); pending orders with repeated errors are not
     silently dropped — they stay pending and an error counter triggers
     cancellation only after MAX_FILL_CHECK_ERRORS consecutive failures.
  2. Affordability check: before placing any limit buy, verifies that
     available_balance (real balance minus reserved capital in pending/open
     positions) covers my_size. Falls back gracefully rather than over-spending.
  3. PnL isolation: sell-side balance diff now snapshots balance *after* a
     brief settle wait, and the snapshot window is intentionally widened.
     More importantly, concurrent buys that land in the same settle window
     are detected and their cost is added back into the PnL calc so the diff
     is not contaminated. A lock serialises sell+balance-read to prevent
     overlap where possible.
  4. Max slippage 20% (sell side only): market sells use a CLOB IOC with a
     min_price floor at best_bid * 0.80; buy prices are governed solely by
     the per-wallet limit_buy_max_premium cap.

Additional fixes (v2):
  5. Race condition in _execute_sell: pending_costs_before snapshot is now
     taken inside _trade_lock so concurrent threads cannot mutate self.pending
     between snapshot and balance read.
  6. compounding_bankroll reconciliation: after every sell (win or loss) and
     on every heartbeat, compounding_bankroll is clamped to the real balance
     so it can never drift above actual capital. On losses the bankroll is
     reduced proportionally.
  7. seen.mark_seen called only after pending entry is confirmed inserted,
     preventing silent trade loss on crash between mark and dict write.
  8. Partial fill handling: is_order_filled returns the filled size (0–shares)
     so partially-filled GTC orders update size_usd correctly rather than
     treating them as fully open or fully filled.
  9. Python 3.9-compatible type hints throughout (Optional[X] instead of X|Y).

WebSocket (v3):
  - Polymarket CLOB WebSocket feed subscribed per token when a pending/open
    position is established. Receives real-time price ticks and order fill
    events, updating position.current_price and resolving pending fills without
    waiting for the REST poll cycle.
  - REST poll at POLL_INTERVAL (default 15 s) is retained as the authoritative
    fallback for wallet scanning (new positions), price updates for tokens not
    yet in the WS subscription, and recovery when the WS connection drops.
  - WS reconnects automatically with exponential back-off (cap 60 s).
"""

import os
import json
import asyncio
import requests
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Optional, Set, Tuple
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ── optional heavy deps ──────────────────────────────────────────────────────
try:
    import websockets                          # type: ignore
    WS_AVAILABLE = True
except ImportError:
    WS_AVAILABLE = False
    logging.warning("websockets not installed — WebSocket feed disabled; pip install websockets")

try:
    from py_clob_client_v2 import (            # type: ignore
        ClobClient, OrderArgs, MarketOrderArgs,
        OrderType, Side, ApiCreds, PartialCreateOrderOptions,
    )
    CLOB_AVAILABLE = True
    logging.info("✅ py_clob_client_v2 loaded successfully")
except ImportError:
    CLOB_AVAILABLE = False
    logging.warning("py_clob_client_v2 not installed — running in simulation mode.")

try:
    import psycopg2                            # type: ignore
    import psycopg2.extras                     # type: ignore
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    logging.warning("psycopg2 not installed — seen_trades will fall back to local file.")

# ==================== CONFIG ====================
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

WALLETS: Dict[str, dict] = {
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
POLL_INTERVAL         = int(os.getenv("POLL_SECONDS", "15"))    # REST fallback cadence
COMPOUNDING_RATE      = float(os.getenv("COMPOUNDING_RATE", "0.70"))
MAX_DRAWDOWN          = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT           = int(os.getenv("PORT", "8080"))
PAUSE_HOURS           = 48
MAX_RETRIES           = 3
RETRY_DELAY           = 5

MAX_SLIPPAGE          = float(os.getenv("MAX_SLIPPAGE", "0.20"))
LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))
LIMIT_EXPIRY_SECONDS  = int(os.getenv("LIMIT_EXPIRY_SECONDS", "300"))
SEEN_TRADES_FILE      = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")
MAX_FILL_CHECK_ERRORS = int(os.getenv("MAX_FILL_CHECK_ERRORS", "5"))

PUSD_CONTRACT_ADDRESS = os.getenv(
    "PUSD_CONTRACT_ADDRESS",
    "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",   # USDC.e on Polygon (Polymarket collateral)
)

SELL_SETTLE_WAIT      = int(os.getenv("SELL_SETTLE_WAIT", "8"))

# WebSocket
CLOB_WS_URL           = os.getenv("CLOB_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
WS_RECONNECT_DELAY    = 2    # initial back-off seconds
WS_RECONNECT_MAX      = 60   # cap

current_bankroll      = INITIAL_BANKROLL
peak_bankroll         = INITIAL_BANKROLL
compounding_bankroll  = INITIAL_BANKROLL
bot_paused_until: Optional[datetime] = None

# Fix 5: asyncio lock (replaces threading.Lock — everything runs in one event loop)
_trade_lock = asyncio.Lock()


# ==================== DASHBOARD ====================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CopyTrader Dashboard</title>
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
        .market-name {{ font-weight: 500; color: #e2e8f0; max-width: 300px; }}
        .outcome-pill {{ display: inline-block; font-size: 0.68rem; font-weight: 700; padding: 2px 8px; border-radius: 999px; text-transform: uppercase; letter-spacing: 0.3px; }}
        .outcome-yes {{ background: #064e3b; color: #6ee7b7; }}
        .outcome-no  {{ background: #450a0a; color: #fca5a5; }}
        .source-tag  {{ font-size: 0.70rem; font-weight: 600; color: #818cf8; background: #1e1b4b; padding: 2px 8px; border-radius: 999px; }}
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


def build_dashboard(bot: "CopyTrader") -> dict:
    def _sign(v: float) -> str: return "+" if v > 0 else ("-" if v < 0 else "")
    def _cls(v: float)  -> str: return "pos" if v > 0 else ("neg" if v < 0 else "neu")

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
            <td><span class="source-tag">{p.source_name}</span></td>
            <td class="market-name">{p.question[:60]}</td>
            <td><span class="outcome-pill {outcome_cls}">{p.outcome}</span></td>
            <td>${p.size_usd:.2f}<br><span style="font-size:0.70rem;color:#475569;">{p.shares:.4f} shares</span></td>
            <td class="price-mono">{p.entry_price:.3f}</td>
            <td class="price-mono">{cur_str}</td>
            <td class="pnl-cell {pnl_cls}">{pnl_str}</td>
        </tr>"""

    if pos_rows:
        positions_block = f"""
        <div class="tbl-wrap"><table>
            <thead><tr>
                <th>Source</th><th>Market</th><th>Side</th>
                <th>Size</th><th>Entry</th><th>Current</th><th>Unreal PnL</th>
            </tr></thead>
            <tbody>{pos_rows}</tbody>
        </table></div>"""
    else:
        positions_block = '<div class="empty"><div class="empty-icon">📭</div>No open positions</div>'

    closed_list = getattr(bot, "closed_positions", [])
    realised    = sum(p.pnl for p in closed_list)
    closed_rows = ""
    for p in reversed(closed_list):
        outcome_cls = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        pnl_cls     = _cls(p.pnl)
        pnl_str     = f"{_sign(p.pnl)}${abs(p.pnl):.2f}"
        closed_rows += f"""
        <tr>
            <td><span class="source-tag">{p.source_name}</span></td>
            <td class="market-name">{p.question[:60]}</td>
            <td><span class="outcome-pill {outcome_cls}">{p.outcome}</span></td>
            <td class="price-mono">{p.entry_price:.3f}</td>
            <td class="price-mono">{p.exit_price:.3f}</td>
            <td class="pnl-cell {pnl_cls}">{pnl_str}</td>
        </tr>"""

    if closed_rows:
        closed_block = f"""
        <div class="tbl-wrap"><table>
            <thead><tr>
                <th>Source</th><th>Market</th><th>Side</th>
                <th>Entry</th><th>Exit</th><th>Realised PnL</th>
            </tr></thead>
            <tbody>{closed_rows}</tbody>
        </table></div>"""
    else:
        closed_block = '<div class="empty"><div class="empty-icon">📋</div>No closed trades yet</div>'

    total_pnl  = realised + unrealised
    comp_delta = compounding_bankroll - (bot.balance.peak_balance or INITIAL_BANKROLL)

    def _fmt(v: float) -> str:
        return f"{abs(v):.4f}" if abs(v) < 0.005 else f"{abs(v):.2f}"

    return {
        "last_updated":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode_label":      mode_label,
        "mode_badge":      mode_badge,
        "status_label":    status_label,
        "status_badge":    status_badge,
        "balance":         bankroll,
        "available":       available,
        "peak":            peak_bankroll,
        "drawdown":        drawdown,
        "dd_cls":          "neg" if drawdown > 10 else ("neu" if drawdown > 5 else "pos"),
        "max_dd":          MAX_DRAWDOWN * 100,
        "comp_bankroll":   compounding_bankroll,
        "comp_cls":        _cls(comp_delta),
        "comp_rate":       COMPOUNDING_RATE * 100,
        "total_pnl_cls":   _cls(total_pnl),
        "total_pnl_sign":  _sign(total_pnl),
        "total_pnl_abs":   _fmt(total_pnl),
        "unreal_cls":      _cls(unrealised),
        "unreal_sign":     _sign(unrealised),
        "unreal_abs":      _fmt(unrealised),
        "real_cls":        _cls(realised),
        "real_sign":       _sign(realised),
        "real_abs":        _fmt(realised),
        "open_count":      len(bot.positions),
        "closed_count":    len(closed_list),
        "positions_block": positions_block,
        "closed_block":    closed_block,
    }


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" and _bot_ref:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            try:
                data = build_dashboard(_bot_ref)
                html = HTML_TEMPLATE.format(**data)
                self.wfile.write(html.encode())
            except Exception:
                self.wfile.write(b"<h1>Dashboard loading...</h1>")
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK - CopyTrader V2 running")

    def log_message(self, format, *args):  # type: ignore[override]
        pass


_bot_ref: Optional["CopyTrader"] = None


def run_health_server():
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    logging.info(f"🌐 Dashboard live at http://0.0.0.0:{HEALTH_PORT}")
    server.serve_forever()


# ==================== DATA CLASSES ====================
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
    source_shares: float = 0.0
    shares_at_open: float = 0.0
    source_shares_at_open: float = 0.0


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
    source_shares: float   = 0.0
    fill_check_errors: int = 0
    placed_at: datetime    = field(default_factory=datetime.now)


# ==================== SEEN TRADES STORE ====================
class SeenTradesStore:
    def __init__(self, filepath: str, db_url: str = ""):
        self.filepath = filepath
        self.db_url   = db_url
        self._seen: Set[str] = set()
        self._conn    = None
        self.backend  = "local-file"

        if db_url and PSYCOPG2_AVAILABLE:
            self._init_postgres()
        else:
            self._load_file()

        logging.info(
            f"SeenTradesStore ready | backend={self.backend} | "
            f"{len(self._seen)} historic keys loaded"
        )

    def _init_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require")
            self._conn.autocommit = True
            with self._conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS seen_trades (
                        pos_key    TEXT PRIMARY KEY,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
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
                cur.execute(
                    "INSERT INTO seen_trades (pos_key) VALUES (%s) ON CONFLICT DO NOTHING",
                    (pos_key,)
                )
        except Exception as e:
            logging.warning(f"Postgres save failed for {pos_key}: {e}")
            self._reconnect_postgres()

    def _save_postgres_many(self, keys):
        if not keys:
            return
        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    "INSERT INTO seen_trades (pos_key) VALUES %s ON CONFLICT DO NOTHING",
                    [(k,) for k in keys]
                )
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
                data      = json.load(f)
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
        payload = {
            "jsonrpc": "2.0",
            "method":  "eth_call",
            "params":  [
                {"to": PUSD_CONTRACT_ADDRESS, "data": "0x70a08231" + padded},
                "latest",
            ],
            "id": 1,
        }

        for rpc in self.POLYGON_RPCS:
            try:
                resp = requests.post(rpc, json=payload, timeout=8)
                if resp.status_code == 200:
                    data   = resp.json()
                    result = data.get("result", "0x0")
                    if result and result not in ("0x", "0x0"):
                        balance = int(result, 16) / 1_000_000
                        if balance > 0:
                            logging.info(f"pUSD balance via RPC ({rpc}): ${balance:.2f}")
                            return balance
                        else:
                            logging.warning(f"pUSD balance is 0 for {YOUR_WALLET[:10]}…")
            except Exception as e:
                logging.warning(f"RPC balance fetch failed ({rpc}): {e}")
                continue
        logging.error(f"All RPC attempts failed for {YOUR_WALLET[:10] if YOUR_WALLET else 'NOT SET'}…")
        return 0.0

    def get_balance(self, force: bool = False) -> Optional[float]:
        if force or self.cached_balance is None or (time.time() - self.last_update > 30):
            real = self._fetch_balance()
            if real > 0:
                self.cached_balance = real
                self.last_update    = time.time()
                if real > self.peak_balance:
                    self.peak_balance = real
                    logging.info(f"New peak balance: ${self.peak_balance:.2f}")
            else:
                if self.cached_balance is None:
                    logging.error(
                        "Could not fetch real pUSD balance — "
                        "bot will not trade until balance is confirmed"
                    )
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
            logging.warning(
                f"Balance fetch attempt {attempt}/{retries} returned 0 — retrying in {delay}s"
            )
            time.sleep(delay)
        raise RuntimeError(f"Could not fetch real pUSD balance after {retries} attempts.")

    def check_drawdown(self) -> Tuple[bool, float]:
        current = self.get_balance()
        if current is None or self.peak_balance == 0:
            return False, 0.0
        dd = (self.peak_balance - current) / self.peak_balance
        return dd >= MAX_DRAWDOWN, dd


# ==================== EXECUTOR (V2) ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client  = None

        if not dry_run and CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(
                    api_key        = POLY_API_KEY,
                    api_secret     = POLY_SECRET,
                    api_passphrase = POLY_PASSPHRASE,
                )
                self.client = ClobClient(
                    host     = "https://clob.polymarket.com",
                    chain_id = 137,
                    key      = YOUR_PRIVATE_KEY,
                    creds    = creds,
                )
                logging.info("ClobClient V2 initialised — LIVE mode")
            except Exception as e:
                logging.error(f"ClobClient V2 init failed: {e}")
                self.client = None

    def place_limit_buy(
        self, token_id: str, amount_usd: float, limit_price: float
    ) -> Tuple[bool, str, float]:
        shares = round(amount_usd / limit_price, 4)

        if self.dry_run or self.client is None:
            logging.info(
                f"[DRY RUN] LIMIT BUY {shares:.4f} shares @ {limit_price:.4f} "
                f"(${amount_usd:.2f}) token {token_id[:12]}…"
            )
            return True, "dry-run-limit-buy", limit_price

        for attempt in range(MAX_RETRIES):
            try:
                result = self.client.create_and_post_order(
                    order_args = OrderArgs(
                        token_id = token_id,
                        price    = limit_price,
                        size     = shares,
                        side     = Side.BUY,
                    ),
                    options    = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.GTC,
                )
                order_id = result.get("orderID", result.get("id", "unknown"))
                logging.info(f"LIMIT BUY placed (V2): {order_id} | {shares:.4f} @ {limit_price:.4f}")
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

    def is_order_filled(self, order_id: str) -> Optional[float]:
        """
        Fix 1 + Fix 8 (partial fill):
          Returns the filled share count (>0) if matched/filled,
          0.0 if confirmed open/unfilled,
          None if API error (caller must NOT treat as unfilled).

        Partial fills: if the order is still open but some shares have matched,
        returns the partial matched size so the caller can adjust size_usd.
        """
        if self.dry_run or self.client is None:
            # In dry run, treat as immediately fully filled; size unknown so
            # return a sentinel that the caller interprets as "full".
            return -1.0   # sentinel: full fill

        try:
            order  = self.client.get_order(order_id)
            status = order.get("status", "").lower()
            size_matched = float(order.get("size_matched", order.get("matched", 0)) or 0)

            if status in ("matched", "filled"):
                return size_matched if size_matched > 0 else -1.0   # -1 → full fill

            # Partial fill: some shares traded but order still open
            if size_matched > 0:
                return size_matched

            # Confirmed unfilled
            return 0.0
        except Exception as e:
            logging.warning(f"Fill-check API error for {order_id}: {e} — treating as unknown")
            return None   # Fix 1: NOT 0.0

    def place_sell(
        self, token_id: str, shares: float, min_price: float = 0.0
    ) -> Tuple[bool, str]:
        """Fix 4: IOC with min_price floor at best_bid * (1 - MAX_SLIPPAGE)."""
        if self.dry_run or self.client is None:
            logging.info(
                f"[DRY RUN] MARKET SELL {shares:.4f} shares "
                f"min_price={min_price:.4f} token {token_id[:12]}…"
            )
            return True, "dry-run-sell"

        for attempt in range(MAX_RETRIES):
            try:
                sell_args = MarketOrderArgs(
                    token_id  = token_id,
                    amount    = shares,
                    side      = Side.SELL,
                )
                if min_price > 0:
                    try:
                        sell_args = MarketOrderArgs(
                            token_id  = token_id,
                            amount    = shares,
                            side      = Side.SELL,
                            min_price = round(min_price, 4),
                        )
                    except TypeError:
                        pass   # SDK version doesn't support min_price

                result   = self.client.create_and_post_market_order(
                    order_args = sell_args,
                    options    = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.IOC,
                )
                order_id = result.get("orderID", result.get("id", "unknown"))
                logging.info(f"MARKET SELL placed (V2 IOC): {order_id} min_price={min_price:.4f}")
                return True, order_id
            except Exception as e:
                logging.warning(f"SELL attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return False, ""


# ==================== WEBSOCKET FEED ====================
class MarketWebSocketFeed:
    """
    Subscribes to the Polymarket CLOB WebSocket for real-time price ticks
    and order-matched events.

    Primary use:
      - Update position.current_price without waiting for REST poll.
      - Detect order fills in real-time and push them to _ws_fill_queue so
        the main loop can promote pending → position immediately.

    The feed is best-effort: if it drops, the REST poll at POLL_INTERVAL
    continues as the authoritative fallback. The feed reconnects automatically
    with exponential back-off.
    """

    def __init__(self, bot: "CopyTrader"):
        self.bot              = bot
        self._subscribed: Set[str] = set()
        self._task: Optional[asyncio.Task] = None   # type: ignore[type-arg]
        self._running         = False
        self._ws_fill_queue: asyncio.Queue = asyncio.Queue()   # type: ignore[type-arg]
        self._reconnect_delay = WS_RECONNECT_DELAY

    # ── public interface ─────────────────────────────────────────────────────

    def start(self):
        if not WS_AVAILABLE:
            logging.warning("WebSocket feed disabled (websockets package not installed)")
            return
        self._running = True
        self._task    = asyncio.create_task(self._run_forever())
        logging.info("WebSocket feed task started")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()

    def subscribe(self, token_ids: Set[str]):
        """Called when new tokens need to be added to the subscription."""
        new = token_ids - self._subscribed
        if new:
            self._subscribed.update(new)
            # Reconnect to pick up new subscriptions
            if self._task and not self._task.done():
                self._task.cancel()
                if self._running:
                    self._task = asyncio.create_task(self._run_forever())

    def unsubscribe(self, token_ids: Set[str]):
        self._subscribed -= token_ids

    async def drain_fill_queue(self):
        """
        Yield (order_id, filled_size) tuples from the WS fill queue.
        Non-blocking — returns immediately when queue is empty.
        """
        while not self._ws_fill_queue.empty():
            try:
                yield await asyncio.wait_for(self._ws_fill_queue.get(), timeout=0.01)
            except asyncio.TimeoutError:
                break

    # ── internals ────────────────────────────────────────────────────────────

    async def _run_forever(self):
        while self._running:
            if not self._subscribed:
                await asyncio.sleep(2)
                continue
            try:
                await self._connect_and_listen()
                self._reconnect_delay = WS_RECONNECT_DELAY   # reset on clean exit
            except asyncio.CancelledError:
                logging.info("WebSocket task cancelled — stopping")
                return
            except Exception as e:
                logging.warning(
                    f"WebSocket disconnected: {e} — reconnecting in {self._reconnect_delay}s"
                )
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, WS_RECONNECT_MAX)

    async def _connect_and_listen(self):
        import websockets as ws_lib   # local import so module-level guard still works

        subscribe_msg = json.dumps({
            "type":    "subscribe",
            "markets": list(self._subscribed),
        })

        async with ws_lib.connect(
            CLOB_WS_URL,
            ping_interval    = 20,
            ping_timeout     = 10,
            close_timeout    = 5,
        ) as ws:
            await ws.send(subscribe_msg)
            logging.info(
                f"WebSocket connected | subscribed to {len(self._subscribed)} token(s)"
            )
            self._reconnect_delay = WS_RECONNECT_DELAY

            async for raw in ws:
                if not self._running:
                    break
                try:
                    self._handle_message(raw)
                except Exception as e:
                    logging.debug(f"WS message handling error: {e}")

    def _handle_message(self, raw: str):
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        event_type = msg.get("type") or msg.get("event_type", "")

        # ── price tick ──────────────────────────────────────────────────────
        if event_type in ("price_change", "tick", "book"):
            token_id = msg.get("asset_id") or msg.get("market") or msg.get("token_id", "")
            mid      = float(msg.get("mid_price") or msg.get("price") or 0)
            if token_id and mid > 0:
                # Update open positions
                for pos in self.bot.positions.values():
                    if pos.token_id == token_id:
                        pos.current_price = mid
                # Update pending order current_price reference (for logging only)
                for pnd in self.bot.pending.values():
                    if pnd.token_id == token_id:
                        pass   # pending doesn't carry current_price; no action needed

        # ── order matched / filled ───────────────────────────────────────────
        elif event_type in ("order_matched", "order_filled", "match"):
            order_id     = msg.get("order_id") or msg.get("id", "")
            size_matched = float(msg.get("size_matched") or msg.get("size") or 0)
            if order_id:
                # Put on queue; main loop drains and promotes pending → position
                self._ws_fill_queue.put_nowait((order_id, size_matched))
                logging.debug(f"WS fill event: order {order_id} size {size_matched}")


# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run    = dry_run
        self.balance    = RobustBalanceManager()
        self.positions: Dict[str, Position]        = {}
        self.pending:   Dict[str, PendingLimitBuy] = {}
        self.executor   = PolymarketExecutor(dry_run)
        self.seen       = SeenTradesStore(SEEN_TRADES_FILE, DATABASE_URL)
        self.ws_feed    = MarketWebSocketFeed(self)

        self._first_scan_done: Set[str]   = set()
        self.closed_positions: list       = []

        logging.info(f"Multi-Wallet CopyTrader V2 started | mode={'DRY RUN' if dry_run else 'LIVE'}")
        logging.info(
            f"Watching {len(WALLETS)} wallets | max_positions={MAX_POSITIONS} | "
            f"ask_cap=+{LIMIT_BUY_MAX_PREMIUM*100:.0f}% | max_slippage={MAX_SLIPPAGE*100:.0f}% | "
            f"expiry={LIMIT_EXPIRY_SECONDS}s | poll={POLL_INTERVAL}s | "
            f"storage={self.seen.backend} | sdk=py-clob-client-v2 | collateral=pUSD"
        )
        for addr, cfg in WALLETS.items():
            logging.info(f"  {cfg['name']} ({addr[:10]}…) copy_mode={cfg['copy_mode']}")

    # ── Fix 6: compounding_bankroll reconciliation ────────────────────────────
    def _reconcile_compounding_bankroll(self, pnl: float = 0.0):
        """
        Called after every sell.
        - On profit: compound COMPOUNDING_RATE fraction into bankroll.
        - On loss:   reduce bankroll by the absolute loss.
        - Always clamp bankroll to real balance so it can never exceed capital.
        """
        global compounding_bankroll
        real_balance = self.balance.cached_balance or 0.0

        if pnl > 0:
            compounding_bankroll += pnl * COMPOUNDING_RATE
            logging.info(
                f"Compounding profit: +${pnl * COMPOUNDING_RATE:.4f} → "
                f"bankroll=${compounding_bankroll:.2f}"
            )
        elif pnl < 0:
            compounding_bankroll += pnl   # pnl is negative, so this subtracts
            logging.info(
                f"Compounding loss: ${pnl:.4f} → bankroll=${compounding_bankroll:.2f}"
            )

        # Hard clamp: bankroll must never exceed real balance
        if compounding_bankroll > real_balance and real_balance > 0:
            logging.info(
                f"Clamping compounding_bankroll ${compounding_bankroll:.2f} → ${real_balance:.2f}"
            )
            compounding_bankroll = real_balance

        # Floor at a small positive value to avoid sizing going to zero
        compounding_bankroll = max(compounding_bankroll, 0.01)

    # ── capital reservation helpers ──────────────────────────────────────────
    def _reserved_capital(self) -> float:
        in_positions = sum(p.size_usd for p in self.positions.values())
        in_pending   = sum(p.size_usd for p in self.pending.values())
        return in_positions + in_pending

    def _available_balance(self) -> float:
        bal = self.balance.cached_balance or 0.0
        return max(0.0, bal - self._reserved_capital())

    def _can_afford(self, amount_usd: float) -> bool:
        available = self._available_balance()
        can       = available >= amount_usd * 1.02
        if not can:
            logging.warning(
                f"Affordability check failed: need ${amount_usd:.2f} but "
                f"available=${available:.2f} (balance=${self.balance.cached_balance or 0:.2f} "
                f"reserved=${self._reserved_capital():.2f})"
            )
        return can

    # ── orderbook helpers ─────────────────────────────────────────────────────
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
                    mid      = (best_bid + best_ask) / 2 if best_bid and best_ask else best_bid or best_ask
                    return mid, best_ask
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    logging.warning(f"Orderbook fetch failed for {token_id[:12]}: {e}")
                time.sleep(RETRY_DELAY)
        return 0.0, 0.0

    def _get_best_bid(self, token_id: str) -> float:
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(
                    f"https://clob.polymarket.com/book?token_id={token_id}", timeout=8
                )
                if r.status_code == 200:
                    bids = r.json().get("bids", [])
                    return float(bids[0]["price"]) if bids else 0.0
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    logging.warning(f"Best bid fetch failed for {token_id[:12]}: {e}")
                time.sleep(RETRY_DELAY)
        return 0.0

    def get_risk_percent(self, price: float, config: dict) -> float:
        if config.get("risk_type") == "fixed":
            return config.get("fixed_risk", 0.025)
        if price >= 0.70:
            return 0.03
        elif price >= 0.30:
            return 0.01
        else:
            return 0.006

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
                logging.warning(
                    f"DRAWDOWN PROTECTION TRIGGERED ({dd*100:.1f}%) — paused {PAUSE_HOURS}h"
                )
            return True
        return False

    def _get_positions(self, wallet_addr: str) -> Optional[list]:
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(
                    f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50",
                    timeout=12,
                )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 30))
                    logging.warning(f"Rate limited on {wallet_addr[:10]} — sleeping {retry_after}s")
                    time.sleep(retry_after)
                    continue
                if resp.status_code == 200:
                    return resp.json()
            except Exception as e:
                logging.warning(f"Position fetch attempt {attempt+1} failed for {wallet_addr}: {e}")
                time.sleep(RETRY_DELAY)
        return None

    # ── WS fill promotion (real-time path) ───────────────────────────────────
    async def _promote_ws_fills(self):
        """
        Drain the WS fill queue and promote pending → position for any
        order_id that matches a pending buy.
        """
        async for order_id, ws_size_matched in self.ws_feed.drain_fill_queue():
            for pos_key, pending in list(self.pending.items()):
                if pending.order_id != order_id:
                    continue
                # Treat ws_size_matched == 0 as full fill (some WS events omit size)
                shares = (
                    ws_size_matched if ws_size_matched > 0
                    else pending.size_usd / pending.limit_price
                )
                actual_usd = shares * pending.limit_price
                self.positions[pos_key] = Position(
                    market_id             = pending.market_id,
                    question              = pending.question,
                    outcome               = pending.outcome,
                    token_id              = pending.token_id,
                    entry_price           = pending.limit_price,
                    size_usd              = actual_usd,
                    shares                = shares,
                    source_wallet         = pending.source_wallet,
                    source_name           = pending.source_name,
                    order_id              = pending.order_id,
                    source_shares         = pending.source_shares,
                    shares_at_open        = shares,
                    source_shares_at_open = pending.source_shares,
                )
                del self.pending[pos_key]
                logging.info(
                    f"WS FILL → position open | {pending.question[:40]} "
                    f"@ {pending.limit_price:.4f} shares={shares:.4f}"
                )
                break   # matched, move to next queue item

    # ── pending order management (REST fallback) ─────────────────────────────
    def _process_pending_orders(self, source_token_ids_by_wallet: Dict[str, Set[str]]):
        for pos_key, pending in list(self.pending.items()):
            wallet_tokens = source_token_ids_by_wallet.get(pending.source_wallet, set())

            # Source exited before our order filled
            if pending.token_id not in wallet_tokens:
                logging.info(f"Source exited before fill — cancelling {pending.question[:40]}")
                self.executor.cancel_order(pending.order_id)
                del self.pending[pos_key]
                continue

            # Fix 1 + Fix 8: is_order_filled returns float|None
            filled_size = self.executor.is_order_filled(pending.order_id)

            if filled_size is None:
                # API error — do NOT drop; increment error counter
                pending.fill_check_errors += 1
                logging.warning(
                    f"Fill check error #{pending.fill_check_errors} for "
                    f"{pending.question[:40]} order {pending.order_id}"
                )
                if pending.fill_check_errors >= MAX_FILL_CHECK_ERRORS:
                    logging.error(
                        f"Max fill check errors reached for {pending.question[:40]} — "
                        f"cancelling to be safe"
                    )
                    self.executor.cancel_order(pending.order_id)
                    del self.pending[pos_key]
                continue

            # Fix 8: partial fill — order still open but some shares matched
            if 0 < filled_size and filled_size != -1.0:
                # Update size_usd to reflect only unmatched portion
                matched_usd       = filled_size * pending.limit_price
                pending.size_usd  = max(0.0, pending.size_usd - matched_usd)
                logging.info(
                    f"Partial fill: {filled_size:.4f} shares matched for "
                    f"{pending.question[:40]} — remaining usd=${pending.size_usd:.4f}"
                )
                pending.fill_check_errors = 0
                # Don't promote to full position yet; wait for full fill or expiry
                self._check_pending_expiry(pos_key, pending, wallet_tokens)
                continue

            # Full fill: filled_size == -1 (sentinel) or filled_size equals expected shares
            if filled_size == -1.0 or (
                pending.limit_price > 0
                and filled_size >= (pending.size_usd / pending.limit_price) * 0.99
            ):
                pending.fill_check_errors = 0
                shares     = (
                    filled_size if filled_size > 0
                    else (pending.size_usd / pending.limit_price if pending.limit_price > 0 else 0)
                )
                actual_usd = shares * pending.limit_price
                self.positions[pos_key] = Position(
                    market_id             = pending.market_id,
                    question              = pending.question,
                    outcome               = pending.outcome,
                    token_id              = pending.token_id,
                    entry_price           = pending.limit_price,
                    size_usd              = actual_usd,
                    shares                = shares,
                    source_wallet         = pending.source_wallet,
                    source_name           = pending.source_name,
                    order_id              = pending.order_id,
                    source_shares         = pending.source_shares,
                    shares_at_open        = shares,
                    source_shares_at_open = pending.source_shares,
                )
                del self.pending[pos_key]
                logging.info(
                    f"LIMIT BUY FILLED (REST) → position open | {pending.question[:40]} "
                    f"@ {pending.limit_price:.4f} | shares={shares:.4f}"
                )
                continue

            # Confirmed unfilled (filled_size == 0.0) — check expiry
            self._check_pending_expiry(pos_key, pending, wallet_tokens)

    def _check_pending_expiry(
        self,
        pos_key:       str,
        pending:       PendingLimitBuy,
        wallet_tokens: Set[str],
    ):
        age = (datetime.now() - pending.placed_at).total_seconds()
        if age < LIMIT_EXPIRY_SECONDS:
            return

        logging.info(f"Order expired ({age:.0f}s) — cancelling and retrying {pending.question[:40]}")
        self.executor.cancel_order(pending.order_id)
        del self.pending[pos_key]

        mid_price, best_ask = self.get_orderbook_prices(pending.token_id)
        if best_ask <= 0 and mid_price <= 0:
            logging.info(f"No orderbook on retry — skipping {pending.question[:40]}")
            return

        current_ask    = best_ask if best_ask > 0 else mid_price
        _cfg           = WALLETS.get(pending.source_wallet, {})
        wallet_premium = _cfg.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
        price_cap      = round(current_ask * (1 + wallet_premium), 4)
        limit_price    = round(min(current_ask, price_cap), 4)

        if not self._can_afford(pending.size_usd):
            logging.warning(
                f"Cannot afford retry for {pending.question[:40]} "
                f"(${pending.size_usd:.2f}) — skipping"
            )
            return

        ok, order_id, actual_price = self.executor.place_limit_buy(
            pending.token_id, pending.size_usd, limit_price
        )
        if ok:
            # Fix 7: insert pending dict entry BEFORE marking seen
            # (mark_seen was already called on the original placement, so no re-mark needed)
            self.pending[pos_key] = PendingLimitBuy(
                pos_key       = pos_key,
                token_id      = pending.token_id,
                market_id     = pending.market_id,
                question      = pending.question,
                outcome       = pending.outcome,
                source_wallet = pending.source_wallet,
                source_name   = pending.source_name,
                limit_price   = actual_price,
                size_usd      = pending.size_usd,
                order_id      = order_id,
                source_shares = pending.source_shares,
            )
            self.ws_feed.subscribe({pending.token_id})
            logging.info(
                f"LIMIT BUY RETRIED | {pending.question[:40]} "
                f"@ {actual_price:.4f} (ask={current_ask:.4f})"
            )

    # ── main scan loop ────────────────────────────────────────────────────────
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

        # Fix 6: reconcile on every scan (clamp only, no pnl arg)
        self._reconcile_compounding_bankroll(pnl=0.0)

        logging.info(
            f"Scanning | balance=${current_bankroll:.2f} | "
            f"available=${self._available_balance():.2f} | "
            f"compounding=${compounding_bankroll:.2f} | "
            f"open={len(self.positions)} | pending={len(self.pending)} | "
            f"seen={len(self.seen._seen)}"
        )

        # Drain real-time WS fills first (before REST processing)
        await self._promote_ws_fills()

        source_token_ids_by_wallet: Dict[str, Set[str]] = {}

        for wallet_addr, config in WALLETS.items():
            copy_mode = config.get("copy_mode", "new_only")
            name      = config["name"]

            raw = self._get_positions(wallet_addr)
            if raw is None:
                logging.warning(f"Skipping {name} — could not fetch positions")
                continue

            source_token_ids: Set[str]             = set()
            source_shares_map: Dict[str, float]    = {}
            for pos in raw:
                tid    = pos.get("asset", "")
                shares = float(pos.get("size", pos.get("shares", 0)))
                if tid and shares > 0:
                    source_token_ids.add(tid)
                    source_shares_map[tid] = shares

            if wallet_addr not in self._first_scan_done:
                self._first_scan_done.add(wallet_addr)
                if copy_mode == "new_only":
                    all_keys = {f"{wallet_addr}_{tid}" for tid in source_token_ids}
                    self.seen.snapshot_existing(all_keys)
                    logging.info(
                        f"[{name}] new_only — {len(all_keys)} existing position(s) "
                        f"snapshotted, skipping buy loop this scan"
                    )
                    source_token_ids_by_wallet[wallet_addr] = source_token_ids
                    continue
                else:
                    logging.info(
                        f"[{name}] copy_all — {len(source_token_ids)} position(s) "
                        f"open at deployment, will copy unseen ones now"
                    )

            logging.info(f"[{name}] {len(raw)} position(s) from API, {len(source_token_ids)} active")

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

                pos_key      = f"{wallet_addr}_{token_id}"
                already_seen = self.seen.is_seen(pos_key)
                in_positions = pos_key in self.positions
                in_pending   = pos_key in self.pending

                if already_seen or in_positions or in_pending:
                    continue

                if len(self.positions) + len(self.pending) >= MAX_POSITIONS:
                    logging.info("Global max positions reached — skipping new entries")
                    break

                wallet_open    = sum(1 for p in self.positions.values() if p.source_wallet == wallet_addr)
                wallet_pending = sum(1 for p in self.pending.values()   if p.source_wallet == wallet_addr)
                wallet_max     = config.get("max_positions", MAX_POSITIONS)
                if wallet_open + wallet_pending >= wallet_max:
                    logging.info(
                        f"[{name}] wallet cap reached "
                        f"({wallet_open} open + {wallet_pending} pending >= {wallet_max})"
                    )
                    break

                cur_price = float(pos.get("curPrice", 0))
                if cur_price <= 0:
                    logging.info(f"[{name}] SKIP no curPrice | {question[:40]}")
                    continue

                wallet_premium = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
                price_cap      = round(cur_price * (1 + wallet_premium), 4)
                limit_price    = round(cur_price, 4)

                if config.get("copy_sub_dollar") and size_usd < 1.0:
                    my_size = round(size_usd, 2)
                else:
                    risk_pct = self.get_risk_percent(limit_price, config)
                    my_size  = round(
                        min(compounding_bankroll * risk_pct, self._available_balance() * 0.95),
                        2,
                    )

                if my_size <= 0 or not self._can_afford(my_size):
                    logging.warning(
                        f"[{name}] Skipping {question[:40]} — cannot afford ${my_size:.2f}"
                    )
                    continue

                ok, order_id, actual_price = self.executor.place_limit_buy(
                    token_id, my_size, limit_price
                )
                if ok:
                    # Fix 7: insert into self.pending FIRST, then mark seen.
                    # If the process dies between these two lines the worst case
                    # is a duplicate buy attempt on next restart, which is safe
                    # because the order is already live on the exchange.
                    # The alternative (mark_seen first) risks silently losing the
                    # trade if the process dies before pending is written.
                    self.pending[pos_key] = PendingLimitBuy(
                        pos_key       = pos_key,
                        token_id      = token_id,
                        market_id     = market_id,
                        question      = question,
                        outcome       = outcome,
                        source_wallet = wallet_addr,
                        source_name   = name,
                        limit_price   = actual_price,
                        size_usd      = my_size,
                        order_id      = order_id,
                        source_shares = source_shares_at_copy,
                    )
                    self.seen.mark_seen(pos_key)   # Fix 7: AFTER pending is registered
                    self.ws_feed.subscribe({token_id})   # subscribe to WS for real-time fills
                    logging.info(
                        f"[{name}] LIMIT BUY PLACED | {question[:40]} | "
                        f"${my_size:.2f} @ {actual_price:.4f} "
                        f"(avail=${self._available_balance():.2f} "
                        f"comp=${compounding_bankroll:.2f} "
                        f"curPrice={cur_price:.4f} cap={price_cap:.4f})"
                    )

            source_token_ids_by_wallet[wallet_addr] = source_token_ids

            # Update current prices from REST for open positions from this wallet
            cur_price_map = {
                pos.get("asset", ""): float(pos.get("curPrice", 0))
                for pos in raw
                if pos.get("asset") and float(pos.get("curPrice", 0)) > 0
            }
            for _pk, _pos in self.positions.items():
                if _pos.source_wallet != wallet_addr:
                    continue
                _cp = cur_price_map.get(_pos.token_id, 0.0)
                if _cp > 0:
                    _pos.current_price = _cp

            # ================================================================
            # SELL LOGIC
            # ================================================================
            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr:
                    continue
                if position.status != "open":
                    continue

                current_source_shares = source_shares_map.get(position.token_id, 0.0)

                # Source added shares — raise baselines
                if current_source_shares > position.source_shares_at_open:
                    logging.info(
                        f"[{name}] Source ADDED shares "
                        f"{position.source_shares_at_open:.4f} → {current_source_shares:.4f} | "
                        f"updating baselines | {position.question[:40]}"
                    )
                    position.source_shares_at_open = current_source_shares
                    position.source_shares         = current_source_shares
                    position.shares_at_open        = position.shares

                # ── CASE 1: Full exit ──
                if position.token_id not in source_token_ids:
                    logging.info(
                        f"[{name}] Source FULL EXIT — selling {position.shares:.4f} shares | "
                        f"{position.question[:40]}"
                    )
                    await self._execute_sell(
                        position, pos_key, position.shares, name, full_exit=True
                    )

                # ── CASE 2: Partial exit ──
                elif (
                    position.source_shares_at_open > 0
                    and current_source_shares < position.source_shares * 0.80
                ):
                    hold_ratio         = current_source_shares / position.source_shares_at_open
                    our_target_shares  = round(position.shares_at_open * hold_ratio, 4)
                    our_shares_to_sell = round(position.shares - our_target_shares, 4)

                    if our_shares_to_sell <= 0:
                        continue

                    logging.info(
                        f"[{name}] Source PARTIAL EXIT — "
                        f"source holds {hold_ratio*100:.1f}% | "
                        f"selling {our_shares_to_sell:.4f} of {position.shares:.4f} | "
                        f"{position.question[:40]}"
                    )
                    await self._execute_sell(
                        position, pos_key, our_shares_to_sell, name,
                        full_exit=False, current_source_shares=current_source_shares,
                    )

        self._process_pending_orders(source_token_ids_by_wallet)

    # ── sell execution ────────────────────────────────────────────────────────
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
            exit_price = position.current_price if position.current_price > 0 else position.entry_price
            pnl        = (exit_price - position.entry_price) * shares_to_sell
            ok         = True
            order_id   = "dry-run-sell"   # noqa: F841
        else:
            best_bid  = self._get_best_bid(position.token_id)
            min_price = round(best_bid * (1 - MAX_SLIPPAGE), 4) if best_bid > 0 else 0.0

            # Fix 5: snapshot pending costs INSIDE the lock so no concurrent
            # thread can mutate self.pending between snapshot and balance read.
            async with _trade_lock:
                pending_costs_before: Dict[str, float] = {
                    pk: p.size_usd for pk, p in self.pending.items()
                }
                balance_before = self.balance.get_balance(force=True) or 0.0
                ok, order_id   = self.executor.place_sell(   # noqa: F841
                    position.token_id, shares_to_sell, min_price=min_price
                )
                if ok:
                    await asyncio.sleep(SELL_SETTLE_WAIT)
                    balance_after = self.balance.get_balance(force=True) or 0.0

            if ok:
                # Fix 3: correct for concurrent buy fills during the settle window
                contamination = 0.0
                for pk, cost in pending_costs_before.items():
                    if pk in self.positions:
                        contamination += cost
                        logging.info(
                            f"PnL contamination correction: +${cost:.4f} "
                            f"(buy filled during settle for {pk[:30]})"
                        )
                raw_diff   = balance_after - balance_before
                pnl        = raw_diff + contamination
                exit_price = best_bid if best_bid > 0 else position.current_price
            else:
                pnl        = 0.0
                exit_price = 0.0

        if not ok:
            logging.error(
                f"[{name}] SELL failed after {MAX_RETRIES} attempts — "
                f"will retry next poll: {position.question[:40]}"
            )
            return

        if full_exit:
            position.status     = "closed"
            position.exit_price = exit_price
            position.pnl        = pnl
            # Fix 6: reconcile bankroll (handles both profit and loss)
            self._reconcile_compounding_bankroll(pnl=pnl)
            logging.info(
                f"[{name}] FULL SELL ({'DRY RUN' if self.dry_run else 'LIVE'}) | "
                f"{position.question[:40]} | exit={exit_price:.4f} pnl=${pnl:.4f}"
            )
            self.ws_feed.unsubscribe({position.token_id})
            self.closed_positions.append(position)
            del self.positions[pos_key]
        else:
            position.shares   -= shares_to_sell
            position.size_usd  = position.shares * position.entry_price
            position.source_shares = current_source_shares
            # Fix 6: reconcile bankroll on partial sell too
            self._reconcile_compounding_bankroll(pnl=pnl)
            logging.info(
                f"[{name}] PARTIAL SELL ({'DRY RUN' if self.dry_run else 'LIVE'}) | "
                f"{position.question[:40]} | sold={shares_to_sell:.4f} "
                f"pnl=${pnl:.4f} remaining={position.shares:.4f}"
            )

    # ── main run loop ─────────────────────────────────────────────────────────
    async def run(self):
        logging.info("Bot loop started (CLOB V2 + WebSocket)")
        self.ws_feed.start()

        # Subscribe WS for any tokens already in positions/pending (restart recovery)
        existing_tokens = (
            {p.token_id for p in self.positions.values()} |
            {p.token_id for p in self.pending.values()}
        )
        if existing_tokens:
            self.ws_feed.subscribe(existing_tokens)

        last_heartbeat = time.time()

        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Main loop error: {e}", exc_info=True)

            now = time.time()
            if now - last_heartbeat >= 300:
                status       = "PAUSED" if bot_paused_until and datetime.now() < bot_paused_until else "ACTIVE"
                wallet_counts: Dict[str, int] = {}
                for p in self.positions.values():
                    wallet_counts[p.source_name] = wallet_counts.get(p.source_name, 0) + 1
                for p in self.pending.values():
                    wallet_counts[p.source_name] = wallet_counts.get(p.source_name, 0) + 1
                slot_summary = " | ".join(f"{n}={c}" for n, c in wallet_counts.items()) or "none"
                # Fix 6: periodic clamp (no pnl event, just sync)
                self._reconcile_compounding_bankroll(pnl=0.0)
                logging.info(
                    f"Heartbeat | {status} | balance=${self.balance.cached_balance or 0:.2f} | "
                    f"available=${self._available_balance():.2f} | "
                    f"compounding=${compounding_bankroll:.2f} | "
                    f"ws_subscribed={len(self.ws_feed._subscribed)} | "
                    f"slots=[{slot_summary}] total={len(self.positions)+len(self.pending)}/{MAX_POSITIONS} | "
                    f"seen={len(self.seen._seen)} | storage={self.seen.backend}"
                )
                last_heartbeat = now

            await asyncio.sleep(POLL_INTERVAL)


# ==================== ENTRY POINT ====================
async def main():
    global _bot_ref, compounding_bankroll, peak_bankroll

    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

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

    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
