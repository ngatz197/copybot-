"""
services.py — Polymarket trade monitoring & limit-order copy execution
Optimized with real-time Event-Driven WebSockets to eliminate 404 Gateway Errors.
"""

import asyncio
import logging
import time
import json
from dataclasses import dataclass, field
from typing import Optional

import httpx
from web3 import Web3

import config

logger = logging.getLogger(__name__)

# ==================== STREAMING CHANNELS / TARGET ENDPOINTS ====================
WS_URL_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class PolyTrade:
    """A detected trade from a source wallet."""
    wallet:          str
    wallet_label:    str           # human label e.g. "RN", "Kruto"
    market_id:       str
    token_id:       str
    side:            str           # "BUY" or "SELL"
    size_usdc:       float
    price:           float         # 0–1 implied probability
    outcome:         str           # "YES" or "NO"
    market_question: str = ""
    market_slug:     str = ""
    tx_hash:         str = ""
    timestamp:       int = 0

@dataclass
class OpenOrder:
    """A limit order we placed that may still be resting."""
    order_id:    str
    trade:       PolyTrade
    limit_price: float
    size_usdc:   float
    placed_at:   int               # unix timestamp

@dataclass
class ClosedTrade:
    """A completed (sold) position used for PnL and win-rate tracking."""
    outcome:     str
    entry_price: float
    exit_price:  float
    pnl:         float             # positive = win, negative = loss

@dataclass
class WalletStats:
    """Per-source-wallet performance counters."""
    wins:         int   = 0
    losses:       int   = 0
    total_pnl:    float = 0.0
    open:         int   = 0        # open positions attributed to this wallet
    closed_trades: list = field(default_factory=list)   # list[ClosedTrade] (last 5)

MAX_POSITIONS = 20  # hard cap on concurrent open positions

@dataclass
class BotState:
    running:         bool  = False
    total_copied:    int   = 0
    total_skipped:   int   = 0
    # market_id:outcome → {"size": usdc, "entry_price": float, "wallet_label": str}
    positions:       dict  = field(default_factory=dict)
    # tx_hash dedup
    seen_txs:        set   = field(default_factory=set)
    # order_id → OpenOrder  (for TTL cancellation)
    open_orders:     dict  = field(default_factory=dict)
    # wallet_label → WalletStats
    wallet_stats:    dict  = field(default_factory=dict)
    # [(timestamp, cumulative_pnl), ...] for chart
    pnl_history:     list  = field(default_factory=list)
    total_pnl:       float = 0.0
    realised_pnl:    float = 0.0   # sum of closed trade PnL
    peak_balance:    float = 0.0   # highest virtual_balance seen, for drawdown calc
    # Real on-chain USDC balance (refreshed every poll cycle)
    real_balance:    float = 0.0
    # Virtual balance: starts equal to real balance at boot, then tracks
    # spending (BUYs deduct) and receipts (SELLs credit) in-memory.
    virtual_balance: float = 0.0
    # Set to True once virtual_balance has been seeded from real_balance
    balance_seeded:  bool  = False

state = BotState()

# Token IDs that returned 404 from the CLOB — market resolved/delisted.
_dead_tokens: set[str] = set()

def _wallet_stats(label: str) -> WalletStats:
    """Return (creating if needed) the WalletStats for a wallet label."""
    if label not in state.wallet_stats:
        state.wallet_stats[label] = WalletStats()
    return state.wallet_stats[label]

# ── Wallet label lookup ───────────────────────────────────────────────────────

WALLET_LABEL = {v.lower(): k for k, v in config.SOURCE_WALLETS.items()}

def label(wallet: str) -> str:
    return WALLET_LABEL.get(wallet.lower(), wallet[:6] + "…")

# ── Polymarket API helpers ────────────────────────────────────────────────────

async def fetch_wallet_trades(
    client: httpx.AsyncClient,
    wallet: str,
    since_ts: int,
) -> list[dict]:
    """Fallback REST poller to confirm state coherence."""
    url = "https://data-api.polymarket.com/trades"
    params = {
        "user":   wallet,
        "tAfter": since_ts,
        "limit":  50,
    }
    try:
        r = await client.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else data.get("data", [])
    except Exception as e:
        logger.warning("fetch_wallet_trades(%s): %s", label(wallet), e)
        return []


async def fetch_market_info(client: httpx.AsyncClient, market_id: str) -> dict:
    url = f"{config.POLYMARKET_GAMMA_API}/markets"
    try:
        r = await client.get(url, params={"condition_id": market_id}, timeout=10)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            return data[0] if data else {}
        return data
    except Exception as e:
        logger.warning("fetch_market_info(%s): %s", market_id, e)
        return {}


async def fetch_order_book(
    client: httpx.AsyncClient,
    token_id: str,
) -> dict:
    if token_id in _dead_tokens:
        return {}

    url = f"{config.POLYMARKET_CLOB_API}/book"
    try:
        r = await client.get(url, params={"token_id": token_id}, timeout=10)
        if r.status_code == 404:
            _dead_tokens.add(token_id)
            logger.info("Order book 404 — market resolved/delisted: …%s", token_id[-12:])
            return {}
        r.raise_for_status()
        return r.json()
    except httpx.HTTPStatusError as err:
        logger.warning("fetch_order_book HTTP error for token …%s: %s", token_id[-12:], err.response.status_code)
        return {}
    except Exception as e:
        logger.warning("fetch_order_book(…%s): %s", token_id[-12:], e)
        return {}


def best_ask(book: dict) -> Optional[float]:
    asks = book.get("asks", [])
    return float(asks[0]["price"]) if asks else None

def best_bid(book: dict) -> Optional[float]:
    bids = book.get("bids", [])
    return float(bids[0]["price"]) if bids else None

# ── Wallet pUSD balance ───────────────────────────────────────────────────────

PUSD_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
PUSD_ABI = [{
    "inputs": [{"name": "account", "type": "address"}],
    "name": "balanceOf",
    "outputs": [{"name": "", "type": "uint256"}],
    "stateMutability": "view",
    "type": "function",
}]

def fetch_wallet_usdc_balance() -> float:
    if not config.DEPOSIT_WALLET_ADDRESS:
        logger.warning("DEPOSIT_WALLET_ADDRESS not set — balance unavailable")
        return state.real_balance or 0.0
    try:
        w3 = Web3(Web3.HTTPProvider(config.POLYGON_RPC_URL, request_kwargs={"timeout": 10}))
        addr = Web3.to_checksum_address(config.DEPOSIT_WALLET_ADDRESS)
        contract = w3.eth.contract(address=Web3.to_checksum_address(PUSD_CONTRACT), abi=PUSD_ABI)
        raw = contract.functions.balanceOf(addr).call()
        balance = raw / 1_000_000
        return balance
    except Exception as e:
        logger.warning("fetch_wallet_usdc_balance (pUSD): %s — using last known balance", e)
        return state.real_balance or 0.0

# ── Trade parsing ─────────────────────────────────────────────────────────────

def parse_trade(raw: dict, wallet: str) -> Optional[PolyTrade]:
    try:
        side      = raw.get("side", "").upper()
        size      = float(raw.get("size", raw.get("usdcSize", 0)))
        price     = float(raw.get("price", raw.get("avgPrice", 0)))
        token_id  = raw.get("asset", raw.get("asset_id", ""))
        market_id = raw.get("conditionId", raw.get("market", raw.get("condition_id", "")))
        outcome   = raw.get("outcome", "")
        tx_hash   = raw.get("transactionHash", raw.get("transaction_hash", ""))
        timestamp = int(raw.get("timestamp", 0))
        title     = raw.get("title", "")
        slug      = raw.get("slug", raw.get("eventSlug", ""))

        if size < config.MIN_SOURCE_TRADE_USDC:
            return None
        if not side or not token_id or not market_id:
            return None
        if not config.COPY_EXITS and side == "SELL":
            return None
        if token_id in _dead_tokens:
            return None

        return PolyTrade(
            wallet=wallet,
            wallet_label=label(wallet),
            market_id=market_id,
            token_id=token_id,
            side=side,
            size_usdc=size,
            price=price,
            outcome=outcome,
            market_question=title,
            market_slug=slug,
            tx_hash=tx_hash,
            timestamp=timestamp,
        )
    except Exception as e:
        logger.debug("parse_trade error: %s", e)
        return None

# ── Limit price calculation ───────────────────────────────────────────────────

def compute_limit_price(side: str, book: dict) -> Optional[float]:
    if side == "BUY":
        ask = best_ask(book)
        if ask is None:
            return None
        return round(min(0.99, ask + config.LIMIT_TICK_OFFSET), 4)
    else:
        bid = best_bid(book)
        if bid is None:
            return None
        return round(max(0.01, bid - config.LIMIT_TICK_OFFSET), 4)


def compute_order_size() -> float:
    balance = state.virtual_balance if state.virtual_balance > 0 else state.real_balance
    size    = round(balance * config.TRADE_PCT, 2)
    logger.info("Order size: 1%% of $%.2f (virtual) = $%.2f", balance, size)
    return size

# ── Order placement ───────────────────────────────────────────────────────────

async def place_limit_order(
    client: httpx.AsyncClient,
    trade: PolyTrade,
    size_usdc: float,
    limit_price: float,
) -> dict:
    shares = round(size_usdc / limit_price, 2)
    if config.DRY_RUN:
        order_id = f"dry-{trade.tx_hash[:8]}-{int(time.time())}"
        logger.info(
            "[DRY-RUN] LIMIT %s %s | %.2f shares @ %.4f ($%.2f) | market=%s",
            trade.side, trade.outcome, shares, limit_price, size_usdc, trade.market_id,
        )
        return {"status": "dry_run", "order_id": order_id, "price": limit_price, "shares": shares}
    raise RuntimeError("Set DRY_RUN=false and configure py-clob-client credentials to run live infrastructure.")


async def cancel_order(client: httpx.AsyncClient, order_id: str) -> None:
    if config.DRY_RUN:
        logger.info("[DRY-RUN] Would cancel order %s", order_id)
        return

# ── TTL watchdog ──────────────────────────────────────────────────────────────

async def ttl_watchdog(client: httpx.AsyncClient) -> None:
    if config.LIMIT_ORDER_TTL_SEC == 0:
        return
    while state.running:
        await asyncio.sleep(10)
        now = int(time.time())
        expired = [
            oid for oid, o in list(state.open_orders.items())
            if now - o.placed_at >= config.LIMIT_ORDER_TTL_SEC
        ]
        for oid in expired:
            o = state.open_orders.pop(oid)
            logger.info("TTL expired — cancelling order %s", oid)
            await cancel_order(client, oid)

# ── Position tracking ─────────────────────────────────────────────────────────

def update_position(trade: PolyTrade, size_usdc: float) -> None:
    key = f"{trade.market_id}:{trade.outcome}"
    ws = _wallet_stats(trade.wallet_label)

    if trade.side == "BUY":
        existing = state.positions.get(key)
        shares = round(size_usdc / trade.price, 4) if trade.price else 0
        if existing is None:
            state.positions[key] = {
                "size":            size_usdc,
                "entry_price":     trade.price,
                "wallet_label":    trade.wallet_label,
                "outcome":         trade.outcome,
                "market_question": trade.market_question,
                "shares":          shares,
                "token_id":        trade.token_id,
            }
            ws.open += 1
        else:
            total = existing["size"] + size_usdc
            existing["entry_price"] = (existing["entry_price"] * existing["size"] + trade.price * size_usdc) / total
            existing["size"]   = total
            existing["shares"] = existing.get("shares", 0) + shares
        
        state.virtual_balance = max(0.0, state.virtual_balance - size_usdc)
        total_value = state.virtual_balance + sum(p["size"] for p in state.positions.values())
        state.peak_balance = max(state.peak_balance, total_value)
    else:
        existing = state.positions.pop(key, None)
        if existing:
            entry  = existing["entry_price"]
            exit_p = trade.price
            pnl    = (exit_p - entry) * (existing["size"] / entry) if entry else 0.0

            proceeds = existing["size"] + pnl
            state.virtual_balance = round(state.virtual_balance + proceeds, 2)

            state.total_pnl      += pnl
            state.realised_pnl   += pnl
            state.pnl_history.append((int(time.time()), round(state.total_pnl, 2)))

            ct = ClosedTrade(outcome=trade.outcome, entry_price=entry, exit_price=exit_p, pnl=round(pnl, 2))
            wlabel = existing.get("wallet_label", trade.wallet_label)
            ws_orig = _wallet_stats(wlabel)
            if pnl >= 0:
                ws_orig.wins += 1
            else:
                ws_orig.losses += 1
            ws_orig.total_pnl = round(ws_orig.total_pnl + pnl, 2)
            ws_orig.open = max(0, ws_orig.open - 1)
            ws_orig.closed_trades = ([ct] + ws_orig.closed_trades)[:5]


async def process_trade(client: httpx.AsyncClient, trade: PolyTrade) -> None:
    if trade.tx_hash in state.seen_txs:
        return
    state.seen_txs.add(trade.tx_hash)

    if not trade.market_question:
        trade.market_question = trade.market_id[:20] + "…"

    if trade.side == "BUY" and len(state.positions) >= MAX_POSITIONS:
        logger.info("Position cap reached — skipping BUY %s", trade.market_question)
        state.total_skipped += 1
        return

    book = await fetch_order_book(client, trade.token_id)
    if not book:
        state.total_skipped += 1
        return

    limit_price = compute_limit_price(trade.side, book)
    if limit_price is None:
        state.total_skipped += 1
        return

    size_usdc = compute_order_size()
    result = await place_limit_order(client, trade, size_usdc, limit_price)

    order_id = result.get("order_id", "unknown")
    state.open_orders[order_id] = OpenOrder(
        order_id=order_id, trade=trade, limit_price=limit_price, size_usdc=size_usdc, placed_at=int(time.time())
    )

    update_position(trade, size_usdc)
    state.total_copied += 1

    logger.info(
        "LIMIT ORDER PLACED | %s | %s %s | $%.2f @ %.4f | source=%.4f",
        trade.wallet_label, trade.side, trade.outcome, size_usdc, limit_price, trade.price
    )

# ==================== WEB-SOCKET EVENT LISTENER LOOP ====================
async def run_websocket_listener(client: httpx.AsyncClient):
    """
    Connects directly to the native CLOB subscription network.
    Avoids the data-api endpoint entirely to mitigate 404 connection drops.
    """
    import websockets
    
    target_wallets = {addr.lower() for addr in config.SOURCE_WALLETS.values()}
    delay = 5
    
    while state.running:
        try:
            logger.info(f"Connecting to live CLOB WebSocket stream: {WS_URL_MARKET}")
            async with websockets.connect(WS_URL_MARKET, ping_interval=20, ping_timeout=30) as ws:
                logger.info("Bot A WebSocket connected successfully to streaming cluster! ✅")
                delay = 5 # Reset recovery delay
                
                # Subscribe to the public market channel
                # Filter allocations occur inside the handler layer using configured user mappings
                subscribe_payload = {
                    "type": "subscribe",
                    "channel": "market",
                    "asset_ids": ["*"] # Listen globally to catch new, dynamic user wallet actions
                }
                await ws.send(json.dumps(subscribe_payload))
                
                async for raw_msg in ws:
                    if not state.running:
                        break
                    try:
                        data = json.loads(raw_msg)
                        if not isinstance(data, list):
                            data = [data]
                            
                        for event in data:
                            ev_type = (event.get("event_type") or event.get("type") or "").lower()
                            
                            # Intercept matching executions or fills
                            if ev_type in ("trade", "fill", "order_filled"):
                                maker = (event.get("maker_address") or event.get("maker") or "").lower()
                                taker = (event.get("taker_address") or event.get("taker") or "").lower()
                                
                                target_wallet = None
                                if maker in target_wallets:
                                    target_wallet = maker
                                elif taker in target_wallets:
                                    target_wallet = taker
                                    
                                if target_wallet:
                                    logger.info(f"🎯 [WS EVENT] Detected activity from tracked user wallet: {target_wallet}")
                                    
                                    # Normalize JSON schema properties to pass onto standard processing engine
                                    raw_trade_payload = {
                                        "side": "BUY" if event.get("side") == "BUY" or event.get("maker_side") == "BUY" else "SELL",
                                        "size": float(event.get("size", 0)) * float(event.get("price", 1)),
                                        "price": float(event.get("price", 0)),
                                        "asset": event.get("asset_id") or event.get("market") or "",
                                        "conditionId": event.get("condition_id") or "",
                                        "outcome": event.get("outcome") or "",
                                        "transactionHash": event.get("transaction_hash") or f"ws-{int(time.time())}",
                                        "timestamp": int(time.time()),
                                        "title": event.get("title", "")
                                    }
                                    
                                    trade = parse_trade(raw_trade_payload, target_wallet)
                                    if trade:
                                        await process_trade(client, trade)
                    except Exception as msg_err:
                        logger.error(f"WebSocket payload processing error: {msg_err}")
                        
        except Exception as conn_err:
            logger.warning(f"WebSocket interface dropped connection: {conn_err}. Reconnecting in {delay}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)

# ── Updated Entry Point Target ────────────────────────────────────────────────

async def monitor_loop() -> None:
    logger.info("Monitor initializing execution stack...")

    real = await asyncio.get_event_loop().run_in_executor(None, fetch_wallet_usdc_balance)
    state.real_balance    = round(real, 2)
    state.virtual_balance = round(real, 2)
    state.peak_balance    = round(real, 2)
    state.balance_seeded  = True

    last_balance_poll = 0

    async with httpx.AsyncClient() as client:
        # Launch concurrency jobs
        asyncio.create_task(ttl_watchdog(client))
        asyncio.create_task(run_websocket_listener(client))

        while state.running:
            now = int(time.time())
            if now - last_balance_poll >= 300:
                real = await asyncio.get_event_loop().run_in_executor(None, fetch_wallet_usdc_balance)
                state.real_balance = round(real, 2)
                last_balance_poll  = now

            # Prices payload refresh loop
            for key, pos in list(state.positions.items()):
                tid = pos.get("token_id", "")
                if tid and tid not in _dead_tokens:
                    book = await fetch_order_book(client, tid)
                    mid  = best_ask(book) or best_bid(book)
                    if mid:
                        pos["current_price"] = mid

            await asyncio.sleep(config.POLL_INTERVAL_SEC)


def get_positions_payload() -> list[dict]:
    rows = []
    for key, pos in state.positions.items():
        entry   = pos["entry_price"]
        current = pos.get("current_price", entry)
        shares  = pos.get("shares", round(pos["size"] / entry, 4) if entry else 0)
        unreal  = round((current - entry) * shares, 4)
        rows.append({
            "key":              key,
            "market_question":  pos.get("market_question", key),
            "outcome":          pos.get("outcome", ""),
            "wallet_label":     pos.get("wallet_label", ""),
            "size":             round(pos["size"], 4),
            "shares":           shares,
            "entry_price":      round(entry, 4),
            "current_price":    round(current, 4),
            "unrealised_pnl":   unreal,
        })
    return rows

def get_status_text() -> str:
    mode   = "DRY RUN" if config.DRY_RUN else "LIVE"
    status = "Running" if state.running else "Stopped"
    lines  = [
        f"=== Polymarket Copy Bot ({status} | {mode}) ===",
        f"Wallets: {', '.join(config.SOURCE_WALLETS.keys())}",
        f"Open orders: {len(state.open_orders)}",
    ]
    return "\n".join(lines)
