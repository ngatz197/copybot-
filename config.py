#!/usr/bin/env python3
"""
config.py - Settings & Dashboard Layer
- Centralizes all environment configuration and variables.
- Manages target wallet definitions and specialized per-wallet constraints.
- Hosts the HTTP Server Handler and HTML/CSS structure for the Render web UI.
"""

import os
import time
import logging
from datetime import datetime
from http.server import BaseHTTPRequestHandler
from dotenv import load_dotenv

load_dotenv()

# ==================== GLOBAL CONTROLS ====================
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

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

LIMIT_BUY_MAX_PREMIUM = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))
LIMIT_EXPIRY_SECONDS  = int(os.getenv("LIMIT_EXPIRY_SECONDS", "300"))
SEEN_TRADES_FILE      = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")

PUSD_CONTRACT_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

# ==================== TARGET WALLETS CONFIG ====================
WALLETS = {
    "0x0c0e270cf879583d6a0142fc817e05b768d0434e": {
        "name": "TheSpirit",
        "risk_type": "price_based",
        "copy_mode": "new_only",
    },
    "0xf903c4cd098184e67a06a04f9b8fdb36e7bbe028": {
        "name": "Viser",
        "risk_type": "price_based",
        "copy_mode": "new_only",
    },
    "0xe8ca3f758c93f44f3ec210542ab78afb7c0bcccb": {
        "name": "Kruto",
        "risk_type": "price_based",
        "copy_mode": "new_only",
        "limit_buy_max_premium": 0.10,
        "copy_sub_dollar": True,
    },
    "0xa1795199a227f8d68134f30bf26314a9918c9629": {
        "name": "Coniyr",
        "risk_type": "fixed",
        "fixed_risk": 0.025,
        "copy_mode": "copy_all",
    },
}

# ==================== RUNTIME STATE GLOBALS ====================
peak_bankroll: float = INITIAL_BANKROLL
compounding_bankroll: float = INITIAL_BANKROLL
bot_paused_until: getattr(datetime, "Optional", None) = None
_bot_ref = None

# ==================== WEB DASHBOARD UI ====================
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
        .badge-live  {{ background: #064e3b; color: #6ee7b7; border: 1px solid #065f46; }}
        .badge-dry   {{ background: #1e1b4b; color: #a5b4fc; border: 1px solid #312e81; }}
        .badge-paused{{ background: #450a0a; color: #fca5a5; border: 1px solid #7f1d1d; }}
        .timestamp   {{ font-size: 0.75rem; color: #64748b; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin-bottom: 24px; }}
        .stat-card {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px; padding: 18px 20px; }}
        .stat-label {{ font-size: 0.72rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.6px; color: #64748b; margin-bottom: 6px; }}
        .stat-value {{ font-size: 1.6rem; font-weight: 700; color: #f1f5f9; line-height: 1; }}
        .stat-sub {{ font-size: 0.75rem; color: #475569; margin-top: 5px; }}
        .pos  {{ color: #34d399; }}
        .neg  {{ color: #f87171; }}
        .neu  {{ color: #94a3b8; }}
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
        .market-name {{ max-width: 300px; font-weight: 500; color: #e2e8f0; }}
        .outcome-pill {{ display: inline-block; font-size: 0.68rem; font-weight: 700; padding: 2px 8px; border-radius: 999px; text-transform: uppercase; letter-spacing: 0.3px; }}
        .outcome-yes {{ background: #064e3b; color: #6ee7b7; }}
        .outcome-no  {{ background: #450a0a; color: #fca5a5; }}
        .source-tag {{ font-size: 0.70rem; font-weight: 600; color: #818cf8; background: #1e1b4b; padding: 2px 8px; border-radius: 999px; }}
        .price-mono {{ font-family: 'Courier New', monospace; font-size: 0.80rem; }}
        .pnl-cell {{ font-weight: 700; font-size: 0.83rem; white-space: nowrap; }}
        .empty {{ padding: 32px 20px; text-align: center; color: #334155; font-size: 0.85rem; }}
        .empty-icon {{ font-size: 1.8rem; margin-bottom: 8px; }}
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

    bankroll   = bot.balance.cached_balance or 0.0
    drawdown   = ((peak_bankroll - bankroll) / peak_bankroll * 100) if peak_bankroll > 0 else 0.0
    is_paused  = bool(bot_paused_until and datetime.now() < bot_paused_until)

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
        shares_str  = f"{p.shares:.4f}"

        pos_rows += f"""
        <tr>
            <td><span class="source-tag">{p.source_name}</span></td>
            <td class="market-name">{p.question[:60]}</td>
            <td><span class="outcome-pill {outcome_cls}">{p.outcome}</span></td>
            <td>${p.size_usd:.2f}<br><span style="font-size:0.70rem;color:#475569;">{shares_str} shares</span></td>
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

    def _fmt(v):
        return f"{abs(v):.4f}" if abs(v) < 0.005 else f"{abs(v):.2f}"

    comp_delta = compounding_bankroll - peak_bankroll

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

    def log_message(self, format, *args):
        pass
