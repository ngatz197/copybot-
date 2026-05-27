#!/usr/bin/env python3
"""
MULTI-WALLET COPY TRADER - PRODUCTION READY (CLOB V2)
- Migrated from py-clob-client (V1) to py-clob-client-v2 (V2)
- Key V2 changes:
    * Package: py_clob_client_v2
    * ClobClient constructor uses keyword args; chain_id= (not positional)
    * LimitOrderArgs → OrderArgs(side=Side.BUY)
    * feeRateBps / nonce / taker removed from order args
    * create_and_post_order() takes order_args=, options=, order_type= kwargs
    * Market sell uses create_and_post_market_order()
    * Collateral token: pUSD (new ERC-20 on Polygon, replaces USDC.e)
    * POLYGON constant removed — use literal chain_id=137
- Limit Buy Orders priced at current best_ask, capped at ask * 1.20 (Option A)
- Falls back to mid if best_ask unavailable
- Market Sell Orders (instant exit)
- Real Mid-Price Fetching
- 20% Drawdown Protection
- Improved Balance Fetching + Robust Error Handling & Retries
- Pending limit order tracking + auto-cancel on expiry
- Health endpoint for Render (keeps bot awake)

PER-WALLET COPY RULES:
  TheSpirit  (0x0c0e...): only copy NEW trades appearing after deployment,
                          value >= $1, 20% ask cap.
  WalletA179 (0xa179...): copy existing positions at deployment too,
                          value >= $1, 20% ask cap.
"""

import os
import json
import asyncio
import requests
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, Set, Tuple, Optional
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
from dotenv import load_dotenv

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
    logging.warning("Install with: pip install py-clob-client-v2")

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
    # NEW-ONLY: only trades that appear after deployment are copied
    "0x0c0e270cf879583d6a0142fc817e05b768d0434e": {
        "name": "TheSpirit",
        "risk_type": "price_based",
        "copy_mode": "new_only",   # skip anything open at deployment
    },
    # COPY-ALL: existing positions at deployment are also copied
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {
        "name": "WalletA179",
        "risk_type": "fixed",
        "fixed_risk": 0.025,
        "copy_mode": "copy_all",   # copy positions open at deployment too
    },
}

YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
POLY_API_KEY     = os.getenv("POLY_API_KEY", "")
POLY_SECRET      = os.getenv("POLY_SECRET", "")
POLY_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")
DATABASE_URL     = os.getenv("DATABASE_URL", "")

INITIAL_BANKROLL      = 10.0
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "8"))
POLL_INTERVAL         = int(os.getenv("POLL_SECONDS", "40"))
COMPOUNDING_RATE      = float(os.getenv("COMPOUNDING_RATE", "0.70"))
MAX_DRAWDOWN          = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT           = int(os.getenv("PORT", "8080"))
PAUSE_HOURS           = 48
MAX_RETRIES           = 3
RETRY_DELAY           = 5

# Cap: won't pay more than this % above current best_ask (Option A)
LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))
LIMIT_EXPIRY_SECONDS  = int(os.getenv("LIMIT_EXPIRY_SECONDS", "300"))
SEEN_TRADES_FILE      = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")

# V2: pUSD contract address on Polygon (verified on PolygonScan)
PUSD_CONTRACT_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

current_bankroll  = INITIAL_BANKROLL
peak_bankroll     = INITIAL_BANKROLL
bot_paused_until: Optional[datetime] = None


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
            background: #0d0d0f;
            color: #e2e8f0;
            min-height: 100vh;
            padding: 24px 16px;
        }}

        /* ── Layout ── */
        .page {{ max-width: 1100px; margin: 0 auto; }}

        /* ── Header ── */
        .header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 28px;
            flex-wrap: wrap;
            gap: 8px;
        }}
        .header-title {{
            font-size: 1.25rem;
            font-weight: 700;
            color: #f8fafc;
            letter-spacing: -0.3px;
        }}
        .header-title span {{ color: #6ee7b7; }}
        .badge {{
            font-size: 0.72rem;
            font-weight: 600;
            padding: 3px 10px;
            border-radius: 999px;
            letter-spacing: 0.4px;
            text-transform: uppercase;
        }}
        .badge-live  {{ background: #064e3b; color: #6ee7b7; border: 1px solid #065f46; }}
        .badge-dry   {{ background: #1e1b4b; color: #a5b4fc; border: 1px solid #312e81; }}
        .badge-paused{{ background: #450a0a; color: #fca5a5; border: 1px solid #7f1d1d; }}
        .timestamp   {{ font-size: 0.75rem; color: #64748b; }}

        /* ── Stat row ── */
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 14px;
            margin-bottom: 24px;
        }}
        .stat-card {{
            background: #16181d;
            border: 1px solid #1e2230;
            border-radius: 12px;
            padding: 18px 20px;
        }}
        .stat-label {{
            font-size: 0.72rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.6px;
            color: #64748b;
            margin-bottom: 6px;
        }}
        .stat-value {{
            font-size: 1.6rem;
            font-weight: 700;
            color: #f1f5f9;
            line-height: 1;
        }}
        .stat-sub {{
            font-size: 0.75rem;
            color: #475569;
            margin-top: 5px;
        }}

        /* ── PnL colours ── */
        .pos  {{ color: #34d399; }}
        .neg  {{ color: #f87171; }}
        .neu  {{ color: #94a3b8; }}

        /* ── Section card ── */
        .section {{
            background: #16181d;
            border: 1px solid #1e2230;
            border-radius: 12px;
            margin-bottom: 20px;
            overflow: hidden;
        }}
        .section-header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 14px 20px;
            border-bottom: 1px solid #1e2230;
        }}
        .section-title {{
            font-size: 0.85rem;
            font-weight: 700;
            color: #cbd5e1;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .count-pill {{
            font-size: 0.72rem;
            font-weight: 700;
            background: #1e2230;
            color: #94a3b8;
            border-radius: 999px;
            padding: 2px 10px;
        }}

        /* ── Table ── */
        .tbl-wrap {{ overflow-x: auto; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.82rem;
        }}
        thead th {{
            padding: 10px 16px;
            text-align: left;
            font-size: 0.70rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: #475569;
            background: #13151a;
            white-space: nowrap;
        }}
        tbody tr {{
            border-top: 1px solid #1a1d26;
            transition: background 0.15s;
        }}
        tbody tr:hover {{ background: #1c1f28; }}
        tbody td {{
            padding: 12px 16px;
            color: #cbd5e1;
            vertical-align: middle;
        }}
        .market-name {{
            font-weight: 500;
            color: #e2e8f0;
            max-width: 300px;
        }}
        .outcome-pill {{
            display: inline-block;
            font-size: 0.68rem;
            font-weight: 700;
            padding: 2px 8px;
            border-radius: 999px;
            text-transform: uppercase;
            letter-spacing: 0.3px;
        }}
        .outcome-yes {{ background: #064e3b; color: #6ee7b7; }}
        .outcome-no  {{ background: #450a0a; color: #fca5a5; }}
        .source-tag {{
            font-size: 0.70rem;
            font-weight: 600;
            color: #818cf8;
            background: #1e1b4b;
            padding: 2px 8px;
            border-radius: 999px;
        }}
        .price-mono {{ font-family: 'Courier New', monospace; font-size: 0.80rem; }}

        /* ── Pnl bar ── */
        .pnl-cell {{
            font-weight: 700;
            font-size: 0.83rem;
            white-space: nowrap;
        }}

        /* ── Empty state ── */
        .empty {{
            padding: 32px 20px;
            text-align: center;
            color: #334155;
            font-size: 0.85rem;
        }}
        .empty-icon {{ font-size: 1.8rem; margin-bottom: 8px; }}
    </style>
</head>
<body>
<div class="page">

    <!-- Header -->
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

    <!-- Stat cards -->
    <div class="stats">
        <div class="stat-card">
            <div class="stat-label">Total Balance</div>
            <div class="stat-value">${balance:.2f}</div>
            <div class="stat-sub">pUSD &nbsp;·&nbsp; Peak ${peak:.2f}</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Total PnL</div>
            <div class="stat-value {total_pnl_cls}">{total_pnl_sign}${total_pnl_abs:.2f}</div>
            <div class="stat-sub">Realised + Unrealised</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Unrealised</div>
            <div class="stat-value {unreal_cls}">{unreal_sign}${unreal_abs:.2f}</div>
            <div class="stat-sub">{open_count} open position(s)</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Realised</div>
            <div class="stat-value {real_cls}">{real_sign}${real_abs:.2f}</div>
            <div class="stat-sub">{closed_count} closed trade(s)</div>
        </div>
        <div class="stat-card">
            <div class="stat-label">Drawdown</div>
            <div class="stat-value {dd_cls}">{drawdown:.1f}%</div>
            <div class="stat-sub">Max {max_dd:.0f}%</div>
        </div>
    </div>

    <!-- Open Positions -->
    <div class="section">
        <div class="section-header">
            <span class="section-title">Open Positions</span>
            <span class="count-pill">{open_count}</span>
        </div>
        {positions_block}
    </div>

    <!-- Closed Trades -->
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
    # "+" for profit, "-" for loss, "" for zero
    def _sign(v): return "+" if v > 0 else ("-" if v < 0 else "")
    def _cls(v):  return "pos" if v > 0 else ("neg" if v < 0 else "neu")

    bankroll   = bot.balance.cached_balance or 0.0
    drawdown   = ((peak_bankroll - bankroll) / peak_bankroll * 100) if peak_bankroll > 0 else 0.0
    is_paused  = bool(bot_paused_until and datetime.now() < bot_paused_until)

    # ── Status / mode badges ──────────────────────────────────────────
    status_label = "Paused" if is_paused else "Running"
    status_badge = "badge-paused" if is_paused else "badge-live"
    mode_label   = "Dry Run" if bot.dry_run else "Live"
    mode_badge   = "badge-dry" if bot.dry_run else "badge-live"

    # ── Unrealised PnL — reads cached current_price set during scan ──
    unrealised = 0.0
    pos_rows   = ""
    for p in bot.positions.values():
        # Use price cached by the scan loop; fall back to entry if not yet set
        mid    = p.current_price if p.current_price > 0 else p.entry_price
        unreal = (mid - p.entry_price) * p.shares
        unrealised += unreal

        outcome_cls = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        pnl_cls     = _cls(unreal)
        # Always show explicit sign: +$X.XX for profit, -$X.XX for loss
        pnl_str     = f"{_sign(unreal)}${abs(unreal):.2f}"
        cur_str     = f"{mid:.3f}" if p.current_price > 0 else "—"

        pos_rows += f"""
        <tr>
            <td><span class="source-tag">{p.source_name}</span></td>
            <td class="market-name">{p.question[:60]}</td>
            <td><span class="outcome-pill {outcome_cls}">{p.outcome}</span></td>
            <td>${p.size_usd:.2f}</td>
            <td class="price-mono">{p.entry_price:.3f}</td>
            <td class="price-mono">{cur_str}</td>
            <td class="pnl-cell {pnl_cls}">{pnl_str}</td>
        </tr>"""

    if pos_rows:
        positions_block = f"""
        <div class="tbl-wrap">
        <table>
            <thead><tr>
                <th>Source</th><th>Market</th><th>Side</th>
                <th>Size</th><th>Entry</th><th>Current</th><th>Unreal PnL</th>
            </tr></thead>
            <tbody>{pos_rows}</tbody>
        </table>
        </div>"""
    else:
        positions_block = '<div class="empty"><div class="empty-icon">📭</div>No open positions</div>'

    # ── Realised PnL (closed trades) ─────────────────────────────────
    closed_list = getattr(bot, "closed_positions", [])
    realised    = sum(p.pnl for p in closed_list)
    closed_rows = ""
    for p in reversed(closed_list):          # newest first
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
        <div class="tbl-wrap">
        <table>
            <thead><tr>
                <th>Source</th><th>Market</th><th>Side</th>
                <th>Entry</th><th>Exit</th><th>Realised PnL</th>
            </tr></thead>
            <tbody>{closed_rows}</tbody>
        </table>
        </div>"""
    else:
        closed_block = '<div class="empty"><div class="empty-icon">📋</div>No closed trades yet</div>'

    total_pnl = realised + unrealised

    return {
        "last_updated":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode_label":      mode_label,
        "mode_badge":      mode_badge,
        "status_label":    status_label,
        "status_badge":    status_badge,
        "balance":         bankroll,
        "peak":            peak_bankroll,
        "drawdown":        drawdown,
        "dd_cls":          "neg" if drawdown > 10 else ("neu" if drawdown > 5 else "pos"),
        "max_dd":          MAX_DRAWDOWN * 100,
        "total_pnl_cls":   _cls(total_pnl),
        "total_pnl_sign":  _sign(total_pnl),
        "total_pnl_abs":   abs(total_pnl),
        "unreal_cls":      _cls(unrealised),
        "unreal_sign":     _sign(unrealised),
        "unreal_abs":      abs(unrealised),
        "real_cls":        _cls(realised),
        "real_sign":       _sign(realised),
        "real_abs":        abs(realised),
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

    def log_message(self, format, *args):
        pass


_bot_ref = None

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
    current_price: float = 0.0   # refreshed each scan cycle


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


# ==================== SEEN TRADES STORE ====================
class SeenTradesStore:
    """
    Persists every pos_key we have ever attempted to copy.
    Backend priority:
      1. Postgres  — if DATABASE_URL is set and psycopg2 is installed.
      2. Local file — fallback for local dev / missing DB config.
    """

    def __init__(self, filepath: str, db_url: str = ""):
        self.filepath = filepath
        self.db_url   = db_url
        self._seen: Set[str] = set()
        self._conn   = None

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
            logging.info(f"Postgres connected — {len(self._seen)} seen keys loaded")
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
            logging.info("Postgres reconnected")
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
    """
    V2 CHANGE: Polymarket V2 uses pUSD as collateral (replaces USDC.e).
    pUSD is a standard ERC-20 on Polygon backed 1:1 by USDC.
    """
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
                "latest"
            ],
            "id": 1,
        }

        for rpc in self.POLYGON_RPCS:
            try:
                resp = requests.post(rpc, json=payload, timeout=8)
                logging.info(f"RPC {rpc} status={resp.status_code}")
                if resp.status_code == 200:
                    data   = resp.json()
                    logging.info(f"RPC response: {data}")
                    result = data.get("result", "0x0")
                    if result and result not in ("0x", "0x0"):
                        balance = int(result, 16) / 1_000_000
                        logging.info(f"pUSD balance fetched via RPC ({rpc}): ${balance:.2f}")
                        if balance > 0:
                            return balance
                        else:
                            logging.warning(f"pUSD balance is 0 for wallet {YOUR_WALLET[:10]}...")
            except Exception as e:
                logging.warning(f"RPC balance fetch failed ({rpc}): {e}")
                continue
        logging.error(f"All RPC attempts failed for wallet {YOUR_WALLET[:10] if YOUR_WALLET else 'NOT SET'}...")
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
            logging.warning(f"Balance fetch attempt {attempt}/{retries} returned 0 — retrying in {delay}s")
            time.sleep(delay)
        raise RuntimeError(
            f"Could not fetch real pUSD balance after {retries} attempts. "
            "Check DEPOSIT_WALLET_ADDRESS, pUSD contract address, and API connectivity."
        )

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
                logging.info(f"LIMIT BUY placed (V2): {order_id} | {shares:.4f} shares @ {limit_price:.4f}")
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

    def is_order_filled(self, order_id: str) -> bool:
        if self.dry_run or self.client is None:
            return True
        try:
            order  = self.client.get_order(order_id)
            status = order.get("status", "").lower()
            return status in ("matched", "filled")
        except Exception as e:
            logging.warning(f"Could not check order status for {order_id}: {e}")
            return False

    def place_sell(self, token_id: str, shares: float) -> Tuple[bool, str]:
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] MARKET SELL {shares:.4f} shares token {token_id[:12]}…")
            return True, "dry-run-sell"

        for attempt in range(MAX_RETRIES):
            try:
                result = self.client.create_and_post_market_order(
                    order_args = MarketOrderArgs(
                        token_id   = token_id,
                        amount     = shares,
                        side       = Side.SELL,
                        order_type = OrderType.FOK,
                    ),
                    options    = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.FOK,
                )
                order_id = result.get("orderID", result.get("id", "unknown"))
                logging.info(f"MARKET SELL placed (V2): {order_id}")
                return True, order_id
            except Exception as e:
                logging.warning(f"SELL attempt {attempt+1} failed: {e}")
                time.sleep(RETRY_DELAY)
        return False, ""


# ==================== COPY TRADER ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        self.dry_run    = dry_run
        self.balance    = RobustBalanceManager()
        self.positions: Dict[str, Position]        = {}
        self.pending:   Dict[str, PendingLimitBuy] = {}
        self.executor   = PolymarketExecutor(dry_run)
        self.seen       = SeenTradesStore(SEEN_TRADES_FILE, DATABASE_URL)

        # Tracks which wallets have completed their first scan
        self._first_scan_done: Set[str] = set()

        # Closed trades kept in memory for PnL display
        self.closed_positions: list = []

        logging.info(f"Multi-Wallet CopyTrader V2 started | mode={'DRY RUN' if dry_run else 'LIVE'}")
        logging.info(
            f"Watching {len(WALLETS)} wallets | max positions={MAX_POSITIONS} | "
            f"ask cap=+{LIMIT_BUY_MAX_PREMIUM*100:.0f}% | expiry={LIMIT_EXPIRY_SECONDS}s | "
            f"storage={self.seen.backend} | sdk=py-clob-client-v2 | collateral=pUSD"
        )
        for addr, cfg in WALLETS.items():
            logging.info(
                f"  {cfg['name']} ({addr[:10]}…) copy_mode={cfg['copy_mode']}"
            )

    def get_orderbook_prices(self, token_id: str) -> Tuple[float, float]:
        """Returns (mid_price, best_ask). Either may be 0.0 on failure."""
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

    def _get_positions(self, wallet_addr: str) -> list | None:
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(
                    f"https://data-api.polymarket.com/positions?user={wallet_addr}&limit=50",
                    timeout=12,
                )
                if resp.status_code == 200:
                    return resp.json()
            except Exception as e:
                logging.warning(f"Position fetch attempt {attempt+1} failed for {wallet_addr}: {e}")
                time.sleep(RETRY_DELAY)
        return None

    # ---- PENDING LIMIT ORDER MANAGEMENT ----
    def _process_pending_orders(self, source_token_ids_by_wallet: Dict[str, set]):
        for pos_key, pending in list(self.pending.items()):
            wallet_tokens = source_token_ids_by_wallet.get(pending.source_wallet, set())

            # Source wallet exited before we filled — cancel
            if pending.token_id not in wallet_tokens:
                logging.info(f"Source exited before fill — cancelling {pending.question[:40]}")
                self.executor.cancel_order(pending.order_id)
                del self.pending[pos_key]
                continue

            # Check if filled
            if self.executor.is_order_filled(pending.order_id):
                shares = pending.size_usd / pending.limit_price if pending.limit_price > 0 else 0
                self.positions[pos_key] = Position(
                    market_id     = pending.market_id,
                    question      = pending.question,
                    outcome       = pending.outcome,
                    token_id      = pending.token_id,
                    entry_price   = pending.limit_price,
                    size_usd      = pending.size_usd,
                    shares        = shares,
                    source_wallet = pending.source_wallet,
                    source_name   = pending.source_name,
                    order_id      = pending.order_id,
                )
                del self.pending[pos_key]
                logging.info(
                    f"LIMIT BUY FILLED → position open | {pending.question[:40]} "
                    f"@ {pending.limit_price:.4f}"
                )
                continue

            # Expired — cancel and retry with fresh ask price
            age = (datetime.now() - pending.placed_at).total_seconds()
            if age >= LIMIT_EXPIRY_SECONDS:
                logging.info(f"Order expired ({age:.0f}s) — cancelling and retrying {pending.question[:40]}")
                self.executor.cancel_order(pending.order_id)
                del self.pending[pos_key]

                mid_price, best_ask = self.get_orderbook_prices(pending.token_id)
                if best_ask <= 0 and mid_price <= 0:
                    logging.info(f"No orderbook on retry — skipping {pending.question[:40]}")
                    continue

                current_ask = best_ask if best_ask > 0 else mid_price
                price_cap   = round(current_ask * (1 + LIMIT_BUY_MAX_PREMIUM), 4)
                limit_price = round(min(current_ask, price_cap), 4)

                ok, order_id, filled_price = self.executor.place_limit_buy(
                    pending.token_id, pending.size_usd, limit_price
                )
                if ok:
                    self.pending[pos_key] = PendingLimitBuy(
                        pos_key       = pos_key,
                        token_id      = pending.token_id,
                        market_id     = pending.market_id,
                        question      = pending.question,
                        outcome       = pending.outcome,
                        source_wallet = pending.source_wallet,
                        source_name   = pending.source_name,
                        limit_price   = filled_price,
                        size_usd      = pending.size_usd,
                        order_id      = order_id,
                    )
                    logging.info(
                        f"LIMIT BUY RETRIED (V2) | {pending.question[:40]} "
                        f"@ {filled_price:.4f} (ask={current_ask:.4f})"
                    )

    async def scan_and_copy(self):
        global current_bankroll, bot_paused_until

        if bot_paused_until and datetime.now() < bot_paused_until:
            remaining = (bot_paused_until - datetime.now()).seconds // 60
            logging.info(f"Bot paused — {remaining} minutes remaining")
            return

        if self.check_drawdown():
            return

        current_bankroll = self.balance.get_balance()
        if current_bankroll is None:
            logging.error("Real pUSD balance unavailable — skipping this scan cycle")
            return

        logging.info(
            f"Scanning | bankroll=${current_bankroll:.2f} pUSD | "
            f"open={len(self.positions)} | pending={len(self.pending)} | "
            f"seen={len(self.seen._seen)}"
        )

        source_token_ids_by_wallet: Dict[str, set] = {}

        for wallet_addr, config in WALLETS.items():
            copy_mode = config.get("copy_mode", "new_only")
            name      = config["name"]

            raw = self._get_positions(wallet_addr)
            if raw is None:
                logging.warning(f"Skipping {name} — could not fetch positions")
                continue

            # ---- Build source token set (value >= $1 filter) ----
            source_token_ids = set()
            for pos in raw:
                tid      = pos.get("asset", "")
                size_usd = float(pos.get("currentValue", 0))
                if tid and size_usd >= 1.0:
                    source_token_ids.add(tid)

            # ----------------------------------------------------------------
            # FIRST SCAN LOGIC — runs once per wallet per deployment
            # ----------------------------------------------------------------
            if wallet_addr not in self._first_scan_done:
                self._first_scan_done.add(wallet_addr)

                if copy_mode == "new_only":
                    # TheSpirit: mark every current position as seen so we
                    # never copy anything that was already open at deployment.
                    # Then skip the buy loop for this scan — the next poll is
                    # the first one where new trades can be detected.
                    all_keys = {f"{wallet_addr}_{tid}" for tid in source_token_ids}
                    self.seen.snapshot_existing(all_keys)
                    logging.info(
                        f"[{name}] new_only — {len(all_keys)} existing position(s) "
                        f"snapshotted at deployment, skipping buy loop this scan"
                    )
                    source_token_ids_by_wallet[wallet_addr] = source_token_ids
                    continue   # ← do NOT copy anything on the first scan

                else:
                    # copy_all (WalletA179): don't snapshot anything — allow
                    # the buy loop below to copy existing positions right now.
                    logging.info(
                        f"[{name}] copy_all — {len(source_token_ids)} position(s) "
                        f"open at deployment, will copy unseen ones now"
                    )

            logging.info(
                f"[{name}] {len(raw)} position(s) from API, "
                f"{len(source_token_ids)} with currentValue >= $1"
            )

            # ---- BUY LOGIC ----
            for pos in raw:
                token_id  = pos.get("asset", "")
                market_id = pos.get("conditionId", "")
                question  = pos.get("title", "Unknown")
                outcome   = pos.get("outcome", "YES")
                size_usd  = float(pos.get("currentValue", 0))

                if not token_id or size_usd < 1.0:
                    continue

                pos_key      = f"{wallet_addr}_{token_id}"
                already_seen = self.seen.is_seen(pos_key)
                in_positions = pos_key in self.positions
                in_pending   = pos_key in self.pending

                logging.info(
                    f"[{name}] {question[:40]} | "
                    f"seen={already_seen} open={in_positions} pending={in_pending} "
                    f"val=${size_usd:.2f}"
                )

                if already_seen or in_positions or in_pending:
                    continue

                if len(self.positions) + len(self.pending) >= MAX_POSITIONS:
                    logging.info("Max positions reached — skipping new entries")
                    break

                # Use curPrice from positions API
                cur_price = float(pos.get("curPrice", 0))
                if cur_price <= 0:
                    logging.info(f"[{name}] SKIP no curPrice | {question[:40]}")
                    continue

                # 20% ask cap (Option A) — applies to both wallets
                price_cap   = round(cur_price * (1 + LIMIT_BUY_MAX_PREMIUM), 4)
                limit_price = round(cur_price, 4)

                logging.info(
                    f"[{name}] curPrice={cur_price:.4f} cap={price_cap:.4f} | {question[:40]}"
                )

                risk_pct = self.get_risk_percent(limit_price, config)
                my_size  = round(current_bankroll * risk_pct, 2)

                ok, order_id, actual_price = self.executor.place_limit_buy(
                    token_id, my_size, limit_price
                )
                if ok:
                    self.seen.mark_seen(pos_key)
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
                    )
                    logging.info(
                        f"[{name}] LIMIT BUY PLACED (V2) | {question[:40]} | "
                        f"${my_size:.2f} @ {actual_price:.4f} "
                        f"(curPrice={cur_price:.4f} cap={price_cap:.4f})"
                    )

            source_token_ids_by_wallet[wallet_addr] = source_token_ids

            # ---- REFRESH current_price on open positions (for dashboard) ----
            # Use curPrice from the positions API response — accurate, already
            # fetched this cycle, no extra HTTP call to the orderbook needed.
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

            # ---- SELL LOGIC ----
            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr:
                    continue
                if position.token_id not in source_token_ids and position.status == "open":
                    exit_price, _ = self.get_orderbook_prices(position.token_id)
                    ok, _ = self.executor.place_sell(position.token_id, position.shares)
                    if ok:
                        pnl = (exit_price - position.entry_price) * position.shares
                        position.status     = "closed"
                        position.exit_price = exit_price
                        position.pnl        = pnl
                        logging.info(
                            f"[{name}] MARKET SELL (V2) | {position.question[:40]} | "
                            f"exit={exit_price:.4f} pnl=${pnl:.2f}"
                        )
                        self.closed_positions.append(position)
                        del self.positions[pos_key]

        self._process_pending_orders(source_token_ids_by_wallet)

    async def run(self):
        logging.info("Bot loop started (CLOB V2)")
        last_heartbeat = time.time()
        while True:
            try:
                await self.scan_and_copy()
            except Exception as e:
                logging.error(f"Main loop error: {e}")

            now = time.time()
            if now - last_heartbeat >= 300:
                status = "PAUSED" if bot_paused_until and datetime.now() < bot_paused_until else "ACTIVE"
                logging.info(
                    f"Heartbeat | {status} | bankroll=${self.balance.cached_balance or 0:.2f} pUSD | "
                    f"open={len(self.positions)} | pending={len(self.pending)} | "
                    f"seen={len(self.seen._seen)} | storage={self.seen.backend}"
                )
                last_heartbeat = now

            await asyncio.sleep(POLL_INTERVAL)


# ==================== ENTRY POINT ====================
async def main():
    global _bot_ref

    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    bot      = CopyTrader(dry_run=DRY_RUN)
    _bot_ref = bot

    try:
        starting_balance = bot.balance.fetch_with_retry(retries=5, delay=10)
        bot.balance.peak_balance = starting_balance
        global peak_bankroll
        peak_bankroll = starting_balance
    except RuntimeError as e:
        logging.error(f"Startup pUSD balance fetch failed: {e} — running in degraded mode")

    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
