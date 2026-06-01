import os
import sys
import json
import time
import logging
import asyncio
import traceback
from datetime import datetime, timedelta
from dataclasses import dataclass
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread

import requests
import websockets

# ==================== LOGGING CONFIGURATION ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("PolyCopyTrader")

# ==================== CONFIGURATION & CONSTANTS ====================
POLY_CLOB_API_URL  = "https://clob.polymarket.com"
POLY_WS_ENDPOINT    = "wss://clob.polymarket.com/ws/v2"

# Strategy parameters 
INITIAL_BANKROLL   = float(os.getenv("INITIAL_BANKROLL", "1000.0"))
COMPOUNDING_RATE   = float(os.getenv("COMPOUNDING_RATE", "1.0"))  # 100% of PnL added
MAX_DRAWDOWN       = float(os.getenv("MAX_DRAWDOWN", "0.30"))     # 30% safety halt

# Global operational state
compounding_bankroll = INITIAL_BANKROLL
peak_bankroll        = INITIAL_BANKROLL
bot_paused_until     = None
_bot_ref             = None

@dataclass
class Position:
    token_id: str
    question: str
    outcome: str
    entry_price: float
    current_price: float
    shares: float
    size_usd: float
    source_name: str
    opened_at: datetime

@dataclass
class ClosedPosition:
    token_id: str
    question: str
    outcome: str
    entry_price: float
    exit_price: float
    shares: float
    pnl: float
    source_name: str
    closed_at: datetime

class MockBalance:
    def __init__(self, initial=1000.0):
        self.cached_balance = initial

# ==================== MARKET DATA MANAGER (WEBSOCKETS) ====================
class MarketDataManager:
    def __init__(self):
        self.prices = {}
        self.active_tokens = set()
        self._loop = None
        self._thread = None

    def start(self):
        self._thread = Thread(target=self._run_loop, daemon=True, name="MarketDataThread")
        self._thread.start()

    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._main_handler())

    async def _main_handler(self):
        while True:
            try:
                if not self.active_tokens:
                    await asyncio.sleep(2)
                    continue
                async with websockets.connect(POLY_WS_ENDPOINT, ping_interval=20, ping_timeout=10) as ws:
                    sub_msg = {
                        "type": "subscribe",
                        "channels": ["price_feed"],
                        "market_ids": list(self.active_tokens)
                    }
                    await ws.send(json.dumps(sub_msg))
                    logger.info(f"Subscribed to price feed for {len(self.active_tokens)} markets")
                    
                    while True:
                        msg = await ws.recv()
                        data = json.loads(msg)
                        if isinstance(data, list):
                            for item in data:
                                self._process_price_item(item)
                        elif isinstance(data, dict):
                            self._process_price_item(data)
            except Exception as e:
                logger.error(f"WS stream disconnected ({e}). Reconnecting in 5s...")
                await asyncio.sleep(5)

    def _process_price_item(self, item):
        if item.get("channel") == "price_feed" or "market_id" in item:
            m_id = item.get("market_id")
            price = item.get("price")
            if m_id and price is not None:
                try:
                    self.prices[str(m_id)] = float(price)
                except ValueError:
                    pass

    def track_tokens(self, token_ids: list):
        updated = False
        for t in token_ids:
            t_str = str(t)
            if t_str not in self.active_tokens:
                self.active_tokens.add(t_str)
                updated = True
        if updated and self._loop and self._loop.is_running():
            logger.info("New markets added. Cycling WS connections...")

    def get_current_price(self, token_id: str) -> float:
        return self.prices.get(str(token_id), 0.0)

market_data = MarketDataManager()

# ==================== CORE COPIER ENGINE ====================
class PolyCopyTrader:
    def __init__(self):
        self.dry_run = True
        self.balance = MockBalance(INITIAL_BANKROLL)
        self.positions = {}
        self.closed_positions = []
        global _bot_ref
        _bot_ref = self

    def _available_balance(self) -> float:
        allocated = sum(p.size_usd for p in self.positions.values())
        return max(0.0, (self.balance.cached_balance or 0.0) - allocated)

    def update_tracking(self):
        t_ids = [p.token_id for p in self.positions.values() if p.token_id]
        if t_ids:
            market_data.track_tokens(t_ids)

    def execute_mock_trade(self, source: str, q_text: str, side: str, t_id: str, amt: float):
        global bot_paused_until, compounding_bankroll
        if bot_paused_until and datetime.now() < bot_paused_until:
            return
            
        key = f"{t_id}_{side.upper()}"
        if key in self.positions:
            return

        entry_px = 0.50  # Mock fallback reference execution price
        shares = amt / entry_px
        
        self.positions[key] = Position(
            token_id=t_id, question=q_text, outcome=side.upper(),
            entry_price=entry_px, current_price=entry_px,
            shares=shares, size_usd=amt, source_name=source,
            opened_at=datetime.now()
        )
        self.update_tracking()
        logger.info(f"Mock Position Opened | {source} | {side.upper()} | {q_text[:40]}")

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

    available = bot._available_balance()
    is_paused = bool(bot_paused_until and datetime.now() < bot_paused_until)

    status_label = "Paused" if is_paused else "Running"
    status_badge = "badge-paused" if is_paused else "badge-live"
    mode_label   = "Dry Run" if bot.dry_run else "Live"
    mode_badge   = "badge-dry" if bot.dry_run else "badge-live"

    unrealised = 0.0
    pos_rows   = ""
    
    # Dynamically extract and apply the real-time Polymarket CLOB market value
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

    # Re-calculate core accounting values safely based on current market values
    bankroll  = (bot.balance.cached_balance or 0.0) + unrealised
    global peak_bankroll
    if bankroll > peak_bankroll:
        peak_bankroll = bankroll

    drawdown  = ((peak_bankroll - bankroll) / peak_bankroll * 100) if peak_bankroll > 0 else 0.0
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

def run_dashboard_server(port=8080):
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    logger.info(f"Dashboard UI web server live on port {port}")
    server.serve_forever()

# ==================== MAIN SERVICE EXECUTION ====================
if __name__ == "__main__":
    market_data.start()
    bot = PolyCopyTrader()
    
    # Fire up dashboard UI thread
    srv_thread = Thread(target=run_dashboard_server, daemon=True, name="DashboardServerThread")
    srv_thread.start()
    
    # Keep the mock loop processing or waiting for tasks
    logger.info("Bot components running. Monitoring entries...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Terminating bot application...")
