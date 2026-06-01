import time
import json
import asyncio
import requests
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, Set, Tuple, Optional
from dataclasses import dataclass, field
from http.server import HTTPServer, BaseHTTPRequestHandler
import config as cfg

# ==================== EXECUTOR V2 API DISCOVERY ====================
try:
    from py_clob_client_v2 import (
        ClobClient, OrderArgs, MarketOrderArgs, OrderType, Side, ApiCreds, PartialCreateOrderOptions
    )
    CLOB_AVAILABLE = True
except ImportError:
    CLOB_AVAILABLE = False

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False


# ==================== STRUCTURE SCHEMAS ====================
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
    placed_at: datetime = field(default_factory=datetime.now)


# ==================== STORAGE CONTROLLER ====================
class SeenTradesStore:
    def __init__(self, filepath: str, db_url: str = ""):
        self.filepath = filepath
        self.db_url = db_url
        self._seen: Set[str] = set()
        self._conn = None
        if db_url and PSYCOPG2_AVAILABLE:
            self._init_postgres()
        else:
            self._load_file()
        logging.info(f"SeenTradesStore ready | backend={self.backend} | {len(self._seen)} historic keys loaded")

    def _init_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require")
            self._conn.autocommit = True
            with self._conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS seen_trades (
                        pos_key TEXT PRIMARY KEY,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
            self._seen = self._load_postgres()
            self.backend = "postgres"
        except Exception as e:
            logging.error(f"Postgres fallback to local: {e}")
            self._conn = None
            self._load_file()

    def _load_postgres(self) -> Set[str]:
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT pos_key FROM seen_trades")
                return {row[0] for row in cur.fetchall()}
        except Exception:
            return set()

    def _save_postgres(self, pos_key: str):
        try:
            with self._conn.cursor() as cur:
                cur.execute("INSERT INTO seen_trades (pos_key) VALUES (%s) ON CONFLICT DO NOTHING", (pos_key,))
        except Exception:
            self._reconnect_postgres()

    def _save_postgres_many(self, keys):
        if not keys: return
        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, "INSERT INTO seen_trades (pos_key) VALUES %s ON CONFLICT DO NOTHING", [(k,) for k in keys])
        except Exception:
            self._reconnect_postgres()

    def _reconnect_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require")
            self._conn.autocommit = True
        except Exception:
            pass

    def _load_file(self):
        try:
            with open(self.filepath, "r") as f:
                data = json.load(f)
                self._seen = set(data) if isinstance(data, list) else set()
        except Exception:
            self._seen = set()
        self.backend = "local-file"

    def _save_file(self):
        try:
            with open(self.filepath, "w") as f:
                json.dump(sorted(self._seen), f)
        except Exception:
            pass

    def is_seen(self, pos_key: str) -> bool:
        return pos_key in self._seen

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
        logging.info(f"Snapshot: marked {len(new_keys)} pre-existing trades as seen")

    @property
    def is_empty(self) -> bool:
        return len(self._seen) == 0


# ==================== BALANCE SYNCHRONIZER ====================
class RobustBalanceManager:
    POLYGON_RPCS = [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com",
        "https://polygon.drpc.org",
    ]
    def __init__(self):
        self.cached_balance: Optional[float] = None
        self.last_update = 0
        self.peak_balance = 0.0

    def _fetch_balance(self) -> float:
        if not cfg.YOUR_WALLET: return 0.0
        padded = cfg.YOUR_WALLET.lower().replace("0x", "").zfill(64)
        payload = {
            "jsonrpc": "2.0", "method": "eth_call",
            "params": [{"to": cfg.PUSD_CONTRACT_ADDRESS, "data": "0x70a08231" + padded}, "latest"], "id": 1,
        }
        for rpc in self.POLYGON_RPCS:
            try:
                resp = requests.post(rpc, json=payload, timeout=8)
                if resp.status_code == 200:
                    result = resp.json().get("result", "0x0")
                    if result and result not in ("0x", "0x0"):
                        return int(result, 16) / 1_000_000
            except Exception:
                continue
        return 0.0

    def get_balance(self, force=False) -> Optional[float]:
        if force or self.cached_balance is None or (time.time() - self.last_update > 30):
            real = self._fetch_balance()
            if real > 0:
                self.cached_balance = real
                self.last_update = time.time()
                if real > self.peak_balance:
                    self.peak_balance = real
                    cfg.peak_bankroll = real
            else:
                if self.cached_balance is None:
                    return None
        return self.cached_balance

    def fetch_with_retry(self, retries: int = 5, delay: int = 10) -> float:
        for attempt in range(1, retries + 1):
            val = self._fetch_balance()
            if val > 0:
                self.cached_balance = val
                self.peak_balance = val
                self.last_update = time.time()
                return val
            time.sleep(delay)
        raise RuntimeError("Could not sync pUSD settlement ledger balance from Polygon RPC network.")


# ==================== TRANSACTION EXECUTOR ====================
class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client = None
        if not dry_run and CLOB_AVAILABLE and cfg.YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(api_key=cfg.POLY_API_KEY, api_secret=cfg.POLY_SECRET, api_passphrase=cfg.POLY_PASSPHRASE)
                self.client = ClobClient(host="https://clob.polymarket.com", chain_id=137, key=cfg.YOUR_PRIVATE_KEY, creds=creds)
                logging.info("ClobClient V2 initialised — LIVE mode")
            except Exception as e:
                logging.error(f"ClobClient V2 Engine critical startup fault: {e}")

    def place_limit_buy(self, token_id: str, amount_usd: float, limit_price: float) -> Tuple[bool, str, float]:
        shares = round(amount_usd / limit_price, 4)
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] LIMIT BUY {shares} shares @ {limit_price:.4f} (${amount_usd:.2f})")
            return True, f"sim-buy-{int(time.time())}", limit_price

        for attempt in range(cfg.MAX_RETRIES):
            try:
                result = self.client.create_and_post_order(
                    order_args=OrderArgs(token_id=token_id, price=limit_price, size=shares, side=Side.BUY),
                    options=PartialCreateOrderOptions(tick_size="0.01"), order_type=OrderType.GTC
                )
                return True, result.get("orderID", result.get("id", "unknown")), limit_price
            except Exception as e:
                logging.warning(f"Limit buy retry {attempt+1}: {e}")
                time.sleep(cfg.RETRY_DELAY)
        return False, "", limit_price

    def cancel_order(self, order_id: str) -> bool:
        if self.dry_run or self.client is None: return True
        try:
            self.client.cancel(order_id)
            return True
        except Exception:
            return False

    def is_order_filled(self, order_id: str) -> bool:
        if self.dry_run or self.client is None: return True
        try:
            order = self.client.get_order(order_id)
            return order.get("status", "").lower() in ("matched", "filled")
        except Exception:
            return False

    def place_sell(self, token_id: str, shares: float) -> Tuple[bool, str]:
        if self.dry_run or self.client is None:
            logging.info(f"[DRY RUN] MARKET SELL {shares} shares")
            return True, "sim-sell"
        for attempt in range(cfg.MAX_RETRIES):
            try:
                result = self.client.create_and_post_market_order(
                    order_args=MarketOrderArgs(token_id=token_id, amount=shares, side=Side.SELL),
                    options=PartialCreateOrderOptions(tick_size="0.01"), order_type=OrderType.FOK
                )
                return True, result.get("orderID", result.get("id", "unknown"))
            except Exception as e:
                logging.warning(f"Market exit exception loop retry: {e}")
                time.sleep(cfg.RETRY_DELAY)
        return False, ""


# ==================== ORCHESTRATION PIPELINE ====================
class CopyTrader:
    def __init__(self, dry_run=True):
        self.dry_run = dry_run
        self.balance = RobustBalanceManager()
        self.positions: Dict[str, Position] = {}
        self.pending: Dict[str, PendingLimitBuy] = {}
        self.executor = PolymarketExecutor(dry_run)
        self.seen = SeenTradesStore(cfg.SEEN_TRADES_FILE, cfg.DATABASE_URL)
        self._first_scan_done: Set[str] = set()
        self.closed_positions: list = []

    def check_drawdown(self) -> bool:
        current = self.balance.get_balance()
        if current is None or self.balance.peak_balance == 0: return False
        dd = (self.balance.peak_balance - current) / self.balance.peak_balance
        if dd >= cfg.MAX_DRAWDOWN:
            logging.critical(f"PROTECTION TRIGGERED: Current Drawdown {dd*100:.1f}% exceeds threshold limit.")
            cfg.bot_paused_until = datetime.now() + timedelta(hours=cfg.PAUSE_HOUES)
            return True
        return False

    def _get_positions(self, wallet_addr: str) -> Optional[list]:
        try:
            r = requests.get(f"https://clob.polymarket.com/positions?user={wallet_addr}", timeout=8)
            return r.json() if r.status_code == 200 else None
        except Exception:
            return None

    def get_orderbook_prices(self, token_id: str) -> Tuple[float, float]:
        try:
            r = requests.get(f"https://clob.polymarket.com/book?token_id={token_id}", timeout=5)
            if r.status_code == 200:
                data = r.json()
                best_bid = float(data["bids"][0]["price"]) if data.get("bids") else 0.0
                best_ask = float(data["asks"][0]["price"]) if data.get("asks") else 0.0
                return best_ask, (best_bid + best_ask) / 2 if (best_bid and best_ask) else best_bid or best_ask
        except Exception:
            pass
        return 0.0, 0.0

    def _get_market_metadata(self, token_id: str) -> Tuple[str, str, str]:
        try:
            r = requests.get(f"https://clob.polymarket.com/sampling-token?token_id={token_id}", timeout=5)
            if r.status_code == 200:
                d = r.json()
                return d.get("conditionId", ""), d.get("question", "Unknown Market Option Title"), d.get("outcome", "YES")
        except Exception:
            pass
        return "", "Unknown Polymarket Title Match", "YES"

    async def scan_and_copy(self):
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until: return
        if self.check_drawdown(): return

        current_bal = self.balance.get_balance()
        if current_bal is None: return

        logging.info(f"Scan Run | Net Assets: ${current_bal:.2f} pUSD | Active Orders: {len(self.positions)} | Open Limits: {len(self.pending)}")
        source_token_ids_by_wallet = {}

        # 1. Evaluate pending limit trades
        for pkey, pending in list(self.pending.items()):
            if self.executor.is_order_filled(pending.order_id):
                logging.info(f"Limit Filled confirmation target: {pending.source_name} token {pending.token_id[:12]}")
                shares_filled = round(pending.size_usd / pending.limit_price, 4)
                self.positions[pkey] = Position(
                    market_id=pending.market_id, question=pending.question, outcome=pending.outcome,
                    token_id=pending.token_id, entry_price=pending.limit_price, size_usd=pending.size_usd,
                    shares=shares_filled, source_wallet=pending.source_wallet, source_name=pending.source_name
                )
                del self.pending[pkey]
            elif datetime.now() - pending.placed_at > timedelta(seconds=cfg.LIMIT_EXPIRY_SECONDS):
                logging.info(f"Limit Order Expiry Timeout on key: {pkey}. Cancelling.")
                if self.executor.cancel_order(pending.order_id):
                    del self.pending[pkey]

        # 2. Process source portfolio scans
        for wallet_addr, w_config in cfg.WALLETS.items():
            raw = self._get_positions(wallet_addr)
            if raw is None: continue

            source_token_ids = {pos.get("asset") for pos in raw if pos.get("asset") and float(pos.get("size", pos.get("shares", 0))) > 0}
            logging.info(f"[{w_config['name']}] {len(raw)} position(s) from API, {len(source_token_ids)} with active tokens")

            if wallet_addr not in self._first_scan_done:
                if w_config["copy_mode"] == "new_only":
                    initial_keys = [f"{wallet_addr}_{pos.get('asset')}" for pos in raw if pos.get("asset")]
                    self.seen.snapshot_existing(initial_keys)
                self._first_scan_done.add(wallet_addr)

            for pos in raw:
                token_id = pos.get("asset")
                if not token_id: continue
                shares_source = float(pos.get("size", pos.get("shares", 0)))
                if shares_source <= 0: continue

                pos_key = f"{wallet_addr}_{token_id}"
                if self.seen.is_seen(pos_key) or pos_key in self.positions or pos_key in self.pending:
                    continue

                if len(self.positions) >= cfg.MAX_POSITIONS:
                    logging.warning("Skipping entry trade copy trigger event: Maximum parallel positions limit ceiling met.")
                    continue

                # Meta verification execution payload
                m_id, question, outcome = self._get_market_metadata(token_id)
                best_ask, mid_price = self.get_orderbook_prices(token_id)
                if best_ask == 0: continue

                premium_cap = w_config.get("limit_buy_max_premium", cfg.LIMIT_BUY_MAX_PREMIUM)
                actual_price = best_ask if best_ask <= (mid_price * (1.0 + premium_cap)) else mid_price
                
                # Size computation matrix
                my_size = cfg.compounding_bankroll * w_config.get("fixed_risk", 0.05) if w_config["risk_type"] == "fixed" else (current_bal * 0.10)
                if my_size > current_bal: my_size = current_bal
                if my_size < 1.0 and not w_config.get("copy_sub_dollar", False): continue

                ok, order_id, pricing = self.executor.place_limit_buy(token_id, my_size, actual_price)
                if ok:
                    self.seen.mark_seen(pos_key)
                    self.pending[pos_key] = PendingLimitBuy(
                        pos_key=pos_key, token_id=token_id, market_id=m_id, question=question, outcome=outcome,
                        source_wallet=wallet_addr, source_name=w_config["name"], limit_price=actual_price, size_usd=my_size, order_id=order_id
                    )

            source_token_ids_by_wallet[wallet_addr] = source_token_ids
            cur_price_map = {pos.get("asset"): float(pos.get("curPrice", 0)) for pos in raw if pos.get("asset") and float(pos.get("curPrice", 0)) > 0}
            for _pk, _pos in self.positions.items():
                if _pos.source_wallet == wallet_addr and _pos.token_id in cur_price_map:
                    _pos.current_price = cur_price_map[_pos.token_id]

            # 3. Dynamic Position Reconciliation Matrix (Exits)
            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr: continue
                if position.token_id not in source_token_ids and position.status == "open":
                    exit_price, _ = self.get_orderbook_prices(position.token_id)
                    if exit_price == 0: exit_price = position.entry_price
                    ok, _ = self.executor.place_sell(position.token_id, position.shares)
                    if ok:
                        pnl = (exit_price - position.entry_price) * position.shares
                        position.status, position.exit_price, position.pnl = "closed", exit_price, pnl
                        if pnl > 0:
                            cfg.compounding_bankroll += pnl * cfg.COMCOUNDING_RATE
                        self.closed_positions.append(position)
                        del self.positions[pos_key]
                        logging.info(f"🔴 Position closed out on match tracking for token {position.token_id[:12]}. PnL: ${pnl:.2f}")


# ==================== HTML MONITOR ENGINE ====================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8"><title>CopyTrader Dashboard</title>
    <meta http-equiv="refresh" content="15">
    <style>
        *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: #0d0d0f; color: #e2e8f0; min-height: 100vh; padding: 24px 16px; }}
        .page {{ max-width: 1100px; margin: 0 auto; }}
        .header {{ display: flex; align-items: center; justify-content: space-between; margin-bottom: 28px; flex-wrap: wrap; gap: 8px; }}
        .header-title {{ font-size: 1.25rem; font-weight: 700; color: #f8fafc; letter-spacing: -0.3px; }}
        .header-title span {{ color: #6ee7b7; }}
        .badge {{ font-size: 0.72rem; font-weight: 600; padding: 3px 10px; border-radius: 999px; text-transform: uppercase; }}
        .badge-live {{ background: #064e3b; color: #6ee7b7; border: 1px solid #065f46; }}
        .badge-dry {{ background: #1e1b4b; color: #a5b4fc; border: 1px solid #312e81; }}
        .badge-paused {{ background: #450a0a; color: #fca5a5; border: 1px solid #7f1d1d; }}
        .timestamp {{ font-size: 0.75rem; color: #64748b; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; margin-bottom: 24px; }}
        .stat-card {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px; padding: 18px 20px; }}
        .stat-label {{ font-size: 0.72rem; font-weight: 600; text-transform: uppercase; color: #64748b; margin-bottom: 6px; }}
        .stat-value {{ font-size: 1.6rem; font-weight: 700; color: #f1f5f9; line-height: 1; }}
        .stat-sub {{ font-size: 0.75rem; color: #475569; margin-top: 5px; }}
        .pos {{ color: #34d399; }} .neg {{ color: #f87171; }} .neu {{ color: #94a3b8; }}
        .section {{ background: #16181d; border: 1px solid #1e2230; border-radius: 12px; margin-bottom: 20px; overflow: hidden; }}
        .section-header {{ display: flex; align-items: center; justify-content: space-between; padding: 14px 20px; border-bottom: 1px solid #1e2230; }}
        .section-title {{ font-size: 0.85rem; font-weight: 700; color: #cbd5e1; text-transform: uppercase; }}
        .count-pill {{ font-size: 0.72rem; font-weight: 700; background: #1e2230; color: #94a3b8; border-radius: 999px; padding: 2px 10px; }}
        .tbl-wrap {{ overflow-x: auto; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.82rem; }}
        thead th {{ padding: 10px 16px; text-align: left; font-size: 0.70rem; font-weight: 600; text-transform: uppercase; color: #475569; background: #13151a; }}
        tbody tr {{ border-top: 1px solid #1a1d26; }}
        tbody td {{ padding: 12px 16px; color: #cbd5e1; }}
        .outcome-pill {{ display: inline-block; font-size: 0.68rem; font-weight: 700; padding: 2px 8px; border-radius: 999px; text-transform: uppercase; }}
        .outcome-yes {{ background: #064e3b; color: #6ee7b7; }}
        .outcome-no {{ background: #450a0a; color: #fca5a5; }}
        .source-tag {{ font-size: 0.70rem; font-weight: 600; color: #818cf8; background: #1e1b4b; padding: 2px 8px; border-radius: 999px; }}
        .empty {{ padding: 32px 20px; text-align: center; color: #475569; font-size: 0.85rem; }}
    </style>
</head>
<body>
<div class="page">
    <div class="header">
        <div>
            <div class="header-title">🤖 Poly<span>CopyTrader</span></div>
            <div class="timestamp">Updated {last_updated} &nbsp;·&nbsp; Auto-refresh 15s</div>
        </div>
        <div>
            <span class="badge {mode_badge}">{mode_label}</span>
            <span class="badge {status_badge}">{status_label}</span>
        </div>
    </div>
    <div class="stats">
        <div class="stat-card"><div class="stat-label">Total Balance</div><div class="stat-value">${balance:.2f}</div><div class="stat-sub">pUSD &nbsp;·&nbsp; Peak ${peak:.2f}</div></div>
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

def build_dashboard(bot) -> dict:
    def _sign(v): return "+" if v > 0 else ("-" if v < 0 else "")
    def _cls(v): return "pos" if v > 0 else ("neg" if v < 0 else "neu")

    bankroll = bot.balance.cached_balance or 0.0
    drawdown = ((cfg.peak_bankroll - bankroll) / cfg.peak_bankroll * 100) if cfg.peak_bankroll > 0 else 0.0
    is_paused = bool(cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until)

    status_label = "Paused" if is_paused else "Running"
    status_badge = "badge-paused" if is_paused else "badge-live"
    mode_label = "Dry Run" if bot.dry_run else "Live"
    mode_badge = "badge-dry" if bot.dry_run else "badge-live"

    unrealised = 0.0
    pos_rows = ""
    for p in bot.positions.values():
        mid = p.current_price if p.current_price > 0 else p.entry_price
        unreal = (mid - p.entry_price) * p.shares
        unrealised += unreal

        outcome_cls = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        pnl_fmt = ".4f" if abs(unreal) < 0.005 else ".2f"
        pos_rows += f"""
        <tr>
            <td><span class="source-tag">{p.source_name}</span></td>
            <td>{p.question[:50]}</td>
            <td><span class="outcome-pill {outcome_cls}">{p.outcome}</span></td>
            <td>${p.size_usd:.2f}</td>
            <td>{p.entry_price:.3f}</td>
            <td>{mid:.3f}</td>
            <td class="{_cls(unreal)}">{_sign(unreal)}${abs(unreal):{pnl_fmt}}</td>
        </tr>"""

    positions_block = f'<div class="tbl-wrap"><table><thead><tr><th>Source</th><th>Market</th><th>Side</th><th>Size</th><th>Entry</th><th>Current</th><th>PnL</th></tr></thead><tbody>{pos_rows}</tbody></table></div>' if pos_rows else '<div class="empty">No active tracking positions</div>'

    realised = sum(p.pnl for p in bot.closed_positions)
    closed_rows = ""
    for p in reversed(bot.closed_positions):
        outcome_cls = "outcome-yes" if p.outcome.upper() == "YES" else "outcome-no"
        closed_rows += f"""
        <tr>
            <td><span class="source-tag">{p.source_name}</span></td>
            <td>{p.question[:50]}</td>
            <td><span class="outcome-pill {outcome_cls}">{p.outcome}</span></td>
            <td>{p.entry_price:.3f}</td>
            <td>{p.exit_price:.3f}</td>
            <td class="{_cls(p.pnl)}">{_sign(p.pnl)}${abs(p.pnl):.2f}</td>
        </tr>"""
    closed_block = f'<div class="tbl-wrap"><table><thead><tr><th>Source</th><th>Market</th><th>Side</th><th>Entry</th><th>Exit</th><th>PnL</th></tr></thead><tbody>{closed_rows}</tbody></table></div>' if closed_rows else '<div class="empty">No historic logs closed out.</div>'

    total_pnl = realised + unrealised
    comp_delta = cfg.compounding_bankroll - cfg.INITIAL_BANKROLL

    return {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "mode_label": mode_label, "mode_badge": mode_badge,
        "status_label": status_label, "status_badge": status_badge,
        "balance": bankroll, "peak": cfg.peak_bankroll, "drawdown": drawdown,
        "dd_cls": "neg" if drawdown > 10 else ("neu" if drawdown > 5 else "pos"),
        "max_dd": cfg.MAX_DRAWDOWN * 100, "comp_bankroll": cfg.compounding_bankroll,
        "comp_cls": _cls(comp_delta), "comp_rate": cfg.COMCOUNDING_RATE * 100,
        "total_pnl_cls": _cls(total_pnl), "total_pnl_sign": _sign(total_pnl), "total_pnl_abs": f"{abs(total_pnl):.2f}",
        "unreal_cls": _cls(unrealised), "unreal_sign": _sign(unrealised), "unreal_abs": f"{abs(unrealised):.2f}",
        "real_cls": _cls(realised), "real_sign": _sign(realised), "real_abs": f"{abs(realised):.2f}",
        "open_count": len(bot.positions), "closed_count": len(bot.closed_positions),
        "positions_block": positions_block, "closed_block": closed_block,
    }

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" and cfg._bot_ref:
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            try:
                data = build_dashboard(cfg._bot_ref)
                self.wfile.write(HTML_TEMPLATE.format(**data).encode())
            except Exception:
                self.wfile.write(b"<h1>Error rendering runtime telemetry dashboard metrics...</h1>")
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK - Health active check container validated.")
    def log_message(self, format, *args): pass

def run_health_server():
    server = HTTPServer(("0.0.0.0", cfg.HEALTH_PORT), HealthHandler)
    logging.info(f"🌐 Management metric monitoring dashboard live at internal port: {cfg.HEALTH_PORT}")
    server.serve_forever()