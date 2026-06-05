"""
services.py — Polymarket trade monitoring & limit-order copy execution
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import httpx
from web3 import Web3

import config

logger = logging.getLogger(__name__)

# ── Data models ───────────────────────────────────────────────────────────────

@dataclass
class PolyTrade:
    """A detected trade from a source wallet."""
    wallet:          str
    wallet_label:    str           # human label e.g. "RN", "Kruto"
    market_id:       str
    token_id:        str
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

@dataclass
class BotState:
    running:       bool  = False
    total_copied:  int   = 0
    total_skipped: int   = 0
    # market_id:outcome → {"size": usdc, "entry_price": float, "wallet_label": str}
    positions:     dict  = field(default_factory=dict)
    # tx_hash dedup
    seen_txs:      set   = field(default_factory=set)
    # order_id → OpenOrder  (for TTL cancellation)
    open_orders:   dict  = field(default_factory=dict)
    # wallet_label → WalletStats
    wallet_stats:  dict  = field(default_factory=dict)
    # [(timestamp, cumulative_pnl), ...] for chart
    pnl_history:   list  = field(default_factory=list)
    total_pnl:     float = 0.0

state = BotState()

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
    # Gamma API: public, no auth required.
    # GET /trades?maker_address=<address>&after=<iso-ts>&limit=50
    url = f"{config.POLYMARKET_GAMMA_API}/trades"
    params = {
        "maker_address": wallet,
        "after":         since_ts,
        "limit":         50,
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
    url = f"{config.POLYMARKET_GAMMA_API}/markets/{market_id}"
    try:
        r = await client.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning("fetch_market_info(%s): %s", market_id, e)
        return {}


async def fetch_order_book(
    client: httpx.AsyncClient,
    token_id: str,
) -> dict:
    """Return raw order book dict with 'bids' and 'asks' lists."""
    url = f"{config.POLYMARKET_CLOB_API}/book"
    try:
        r = await client.get(url, params={"token_id": token_id}, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning("fetch_order_book(%s): %s", token_id, e)
        return {}


def best_ask(book: dict) -> Optional[float]:
    asks = book.get("asks", [])
    return float(asks[0]["price"]) if asks else None

def best_bid(book: dict) -> Optional[float]:
    bids = book.get("bids", [])
    return float(bids[0]["price"]) if bids else None


# ── Wallet USDC balance ───────────────────────────────────────────────────────

USDC_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
USDC_ABI = [{
    "inputs": [{"name": "account", "type": "address"}],
    "name": "balanceOf",
    "outputs": [{"name": "", "type": "uint256"}],
    "stateMutability": "view",
    "type": "function",
}]

def fetch_wallet_usdc_balance() -> float:
    """Read USDC balance of DEPOSIT_WALLET_ADDRESS from Polygon. Falls back to MIN_ORDER_USDC."""
    if not config.DEPOSIT_WALLET_ADDRESS:
        logger.warning("DEPOSIT_WALLET_ADDRESS not set — using MIN_ORDER_USDC fallback")
        return config.MIN_ORDER_USDC
    try:
        w3 = Web3(Web3.HTTPProvider(config.POLYGON_RPC_URL))
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(USDC_CONTRACT),
            abi=USDC_ABI,
        )
        raw = contract.functions.balanceOf(
            Web3.to_checksum_address(config.DEPOSIT_WALLET_ADDRESS)
        ).call()
        balance = raw / 1e6
        logger.debug("Wallet USDC balance: $%.2f", balance)
        return balance
    except Exception as e:
        logger.warning("fetch_wallet_usdc_balance: %s — fallback to MIN_ORDER_USDC", e)
        return config.MIN_ORDER_USDC


# ── Trade parsing ─────────────────────────────────────────────────────────────

def parse_trade(raw: dict, wallet: str) -> Optional[PolyTrade]:
    try:
        # CLOB API field names
        side      = raw.get("side", "").upper()                          # BUY / SELL
        size      = float(raw.get("size", raw.get("usdcSize", 0)))       # shares or usdc
        price     = float(raw.get("price", 0))
        token_id  = raw.get("asset_id", raw.get("asset", ""))            # outcome token ID
        market_id = raw.get("market", raw.get("condition_id", ""))
        outcome   = raw.get("outcome", "")
        tx_hash   = raw.get("transaction_hash", raw.get("transactionHash", ""))
        timestamp = int(raw.get("timestamp", 0))

        if size < config.MIN_SOURCE_TRADE_USDC:
            return None
        if not side or not token_id or not market_id:
            return None
        if not config.COPY_EXITS and side == "SELL":
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
            tx_hash=tx_hash,
            timestamp=timestamp,
        )
    except Exception as e:
        logger.debug("parse_trade error: %s", e)
        return None


# ── Limit price calculation ───────────────────────────────────────────────────

def compute_limit_price(side: str, book: dict) -> Optional[float]:
    """
    For a BUY: place just above the best ask (aggressive limit — fills quickly,
    still avoids paying whatever price a market order would).
    For a SELL: place just below the best bid.

    The offset (LIMIT_TICK_OFFSET) is added/subtracted to ensure queue priority
    while capping slippage vs a raw market order.
    """
    if side == "BUY":
        ask = best_ask(book)
        if ask is None:
            return None
        # Clamp to valid range (0.01 – 0.99 on Polymarket)
        return round(min(0.99, ask + config.LIMIT_TICK_OFFSET), 4)
    else:
        bid = best_bid(book)
        if bid is None:
            return None
        return round(max(0.01, bid - config.LIMIT_TICK_OFFSET), 4)


def compute_order_size() -> float:
    """1% of current USDC balance, clamped to min/max guardrails."""
    balance = fetch_wallet_usdc_balance()
    raw  = balance * config.TRADE_PCT
    size = max(config.MIN_ORDER_USDC, min(config.MAX_ORDER_USDC, raw))
    logger.info("Order size: 1%% of $%.2f = $%.2f", balance, size)
    return size


# ── Order placement ───────────────────────────────────────────────────────────

async def place_limit_order(
    client: httpx.AsyncClient,
    trade: PolyTrade,
    size_usdc: float,
    limit_price: float,
) -> dict:
    """
    Post a GTC limit order to the Polymarket CLOB.

    Dry-run:  logs intent, returns a mock result.
    Live:     uses py-clob-client (uncomment block below and set DRY_RUN=false).

    Limit order mechanics:
      - size in *shares* = size_usdc / limit_price
      - order type: GTC (Good-Till-Cancelled)
      - rests in the book until filled or TTL expires
    """
    shares = round(size_usdc / limit_price, 2)

    if config.DRY_RUN:
        order_id = f"dry-{trade.tx_hash[:8]}-{int(time.time())}"
        logger.info(
            "[DRY-RUN] LIMIT %s %s | %.2f shares @ %.4f ($%.2f) | market=%s",
            trade.side, trade.outcome, shares, limit_price, size_usdc, trade.market_id,
        )
        return {"status": "dry_run", "order_id": order_id, "price": limit_price, "shares": shares}

    # ── LIVE (uncomment when DRY_RUN=false) ──────────────────────────────────
    # from py_clob_client_v2.client import ClobClient
    # from py_clob_client_v2.clob_types import OrderArgs, OrderType
    #
    # clob = ClobClient(
    #     host=config.POLYMARKET_CLOB_API,
    #     key=config.PRIVATE_KEY,
    #     chain_id=137,
    # )
    # order_args = OrderArgs(
    #     token_id=trade.token_id,
    #     price=limit_price,
    #     size=shares,
    #     side=trade.side,
    #     order_type=OrderType.GTC,   # Good-Till-Cancelled limit order
    # )
    # resp = clob.create_and_post_order(order_args)
    # return resp
    # ─────────────────────────────────────────────────────────────────────────

    raise RuntimeError("Set DRY_RUN=false and uncomment py-clob-client to go live.")


async def cancel_order(client: httpx.AsyncClient, order_id: str) -> None:
    """Cancel a resting limit order by ID."""
    if config.DRY_RUN:
        logger.info("[DRY-RUN] Would cancel order %s", order_id)
        return

    # ── LIVE ─────────────────────────────────────────────────────────────────
    # from py_clob_client_v2.client import ClobClient
    # clob = ClobClient(host=config.POLYMARKET_CLOB_API,
    #                   key=config.PRIVATE_KEY, chain_id=137)
    # clob.cancel(order_id)
    # ─────────────────────────────────────────────────────────────────────────


# ── TTL watchdog — cancel stale limit orders ──────────────────────────────────

async def ttl_watchdog(client: httpx.AsyncClient) -> None:
    """
    Runs concurrently. Every 10 s, cancels any open limit order older
    than LIMIT_ORDER_TTL_SEC (if TTL > 0).
    """
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
            logger.info(
                "TTL expired — cancelling order %s (%s %s on %s)",
                oid, o.trade.side, o.trade.outcome, o.trade.market_question or o.trade.market_id,
            )
            await cancel_order(client, oid)


# ── Position tracking ─────────────────────────────────────────────────────────

def update_position(trade: PolyTrade, size_usdc: float) -> None:
    key = f"{trade.market_id}:{trade.outcome}"
    ws = _wallet_stats(trade.wallet_label)

    if trade.side == "BUY":
        existing = state.positions.get(key)
        if existing is None:
            state.positions[key] = {
                "size": size_usdc,
                "entry_price": trade.price,
                "wallet_label": trade.wallet_label,
            }
            ws.open += 1
        else:
            # Average into existing position
            total = existing["size"] + size_usdc
            existing["entry_price"] = (
                existing["entry_price"] * existing["size"] + trade.price * size_usdc
            ) / total
            existing["size"] = total
    else:
        existing = state.positions.pop(key, None)
        if existing:
            entry  = existing["entry_price"]
            exit_p = trade.price
            pnl    = (exit_p - entry) * (existing["size"] / entry) if entry else 0.0
            state.total_pnl += pnl
            state.pnl_history.append((int(time.time()), round(state.total_pnl, 2)))

            ct = ClosedTrade(
                outcome=trade.outcome,
                entry_price=entry,
                exit_price=exit_p,
                pnl=round(pnl, 2),
            )
            wlabel = existing.get("wallet_label", trade.wallet_label)
            ws_orig = _wallet_stats(wlabel)
            if pnl >= 0:
                ws_orig.wins += 1
            else:
                ws_orig.losses += 1
            ws_orig.total_pnl = round(ws_orig.total_pnl + pnl, 2)
            ws_orig.open = max(0, ws_orig.open - 1)
            ws_orig.closed_trades = ([ct] + ws_orig.closed_trades)[:5]


# ── Core copy-trade pipeline ──────────────────────────────────────────────────

async def process_trade(client: httpx.AsyncClient, trade: PolyTrade) -> None:
    if trade.tx_hash in state.seen_txs:
        return
    state.seen_txs.add(trade.tx_hash)

    # Enrich market metadata
    market = await fetch_market_info(client, trade.market_id)
    trade.market_question = market.get("question", trade.market_id[:20] + "…")
    trade.market_slug     = market.get("slug", "")

    # Fetch current order book
    book = await fetch_order_book(client, trade.token_id)
    if not book:
        logger.warning("Empty order book for token %s — skipping", trade.token_id)
        state.total_skipped += 1
        return

    # Compute limit price
    limit_price = compute_limit_price(trade.side, book)
    if limit_price is None:
        logger.warning("No %s liquidity for %s — skipping", trade.side, trade.market_question)
        state.total_skipped += 1
        return

    size_usdc = compute_order_size()

    result = await place_limit_order(client, trade, size_usdc, limit_price)

    # Track open order for TTL watchdog
    order_id = result.get("order_id", "unknown")
    state.open_orders[order_id] = OpenOrder(
        order_id=order_id,
        trade=trade,
        limit_price=limit_price,
        size_usdc=size_usdc,
        placed_at=int(time.time()),
    )

    update_position(trade, size_usdc)
    state.total_copied += 1

    source_ref = f"best {'ask' if trade.side == 'BUY' else 'bid'}"
    book_price = best_ask(book) if trade.side == "BUY" else best_bid(book)

    logger.info(
        "LIMIT ORDER | %s [%s] | %s %s | $%.2f @ %.4f | source fill=%.4f | market=%s",
        trade.wallet_label, trade.wallet[:8],
        trade.side, trade.outcome,
        size_usdc, limit_price,
        trade.price,
        trade.market_question,
    )


# ── Main polling loop ─────────────────────────────────────────────────────────

async def monitor_loop() -> None:
    logger.info(
        "Monitor started. Watching: %s",
        ", ".join(f"{lbl} ({addr[:8]}…)" for lbl, addr in config.SOURCE_WALLETS.items()),
    )
    last_poll = int(time.time()) - config.POLL_INTERVAL_SEC

    async with httpx.AsyncClient() as client:
        # Start TTL watchdog as a sibling task
        asyncio.create_task(ttl_watchdog(client))

        while state.running:
            now = int(time.time())

            for lbl, wallet in config.SOURCE_WALLETS.items():
                raw_trades = await fetch_wallet_trades(client, wallet, last_poll)
                for raw in raw_trades:
                    trade = parse_trade(raw, wallet)
                    if trade:
                        await process_trade(client, trade)

            last_poll = now
            await asyncio.sleep(config.POLL_INTERVAL_SEC)

    logger.info("Monitor stopped.")


# ── Status summary ────────────────────────────────────────────────────────────

def get_status_text() -> str:
    mode   = "DRY RUN" if config.DRY_RUN else "LIVE"
    status = "Running" if state.running else "Stopped"
    lines  = [
        f"=== Polymarket Copy Bot ({status} | {mode}) ===",
        f"Wallets: {', '.join(config.SOURCE_WALLETS.keys())}",
        f"Poll:    every {config.POLL_INTERVAL_SEC}s",
        f"Size:    1% of wallet balance (${config.MIN_ORDER_USDC}–${config.MAX_ORDER_USDC})",
        f"Orders:  GTC limit  |  TTL: {config.LIMIT_ORDER_TTL_SEC}s  |  offset: {config.LIMIT_TICK_OFFSET}",
        f"Copied:  {state.total_copied}  |  Skipped: {state.total_skipped}",
        f"Open orders: {len(state.open_orders)}",
    ]
    if state.positions:
        lines.append("Positions:")
        for k, v in state.positions.items():
            lines.append(f"  {k}  ${v:.2f}")
    return "\n".join(lines)
