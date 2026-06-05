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


def _log_task_exception(task: "asyncio.Task") -> None:
    """
    Done-callback attached to every create_task() call.

    asyncio silently discards exceptions in fire-and-forget tasks unless
    something calls task.result().  This callback ensures that any crash
    is at least visible as an ERROR in the logs so Render's log drain
    (and the operator) can spot it.
    """
    try:
        exc = task.exception()
    except asyncio.CancelledError:
        return
    if exc is not None:
        logger.error(
            "Background task '%s' died with an unhandled exception — "
            "this should not happen; please investigate.",
            task.get_name(),
            exc_info=exc,
        )


# ── Singleton CLOB client (live mode only) ────────────────────────────────────

_clob_client = None

def get_clob_client():
    """
    Return a module-level ClobClient, creating it once on first call.
    Reusing a single instance avoids repeated L1/L2 key-negotiation and
    prevents nonce collisions when orders are placed in quick succession.

    Two-level auth required by Polymarket:
      L1 — wallet (EIP-712) signature, supplied via PRIVATE_KEY.
      L2 — HMAC API credentials (key/secret/passphrase).

    L2 credential resolution (in priority order):
      1. Explicit env vars: CLOB_API_KEY + CLOB_SECRET + CLOB_PASSPHRASE
         — used as-is; no network call required.  PRIVATE_KEY is still
         needed for L1 signing of order payloads, but *not* for auth.
      2. Auto-derive from PRIVATE_KEY via create_or_derive_api_creds()
         — original behaviour when the explicit vars are absent.
    """
    global _clob_client
    if _clob_client is None:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        client = ClobClient(
            host=config.POLYMARKET_CLOB_API,
            key=config.PRIVATE_KEY,
            chain_id=137,
        )

        _have_explicit_creds = all([
            config.CLOB_API_KEY,
            config.CLOB_SECRET,
            config.CLOB_PASSPHRASE,
        ])

        try:
            if _have_explicit_creds:
                # Use pre-supplied L2 credentials directly — no round-trip to
                # the CLOB auth endpoint needed.
                creds = ApiCreds(
                    api_key=config.CLOB_API_KEY,
                    api_secret=config.CLOB_SECRET,
                    api_passphrase=config.CLOB_PASSPHRASE,
                )
                client.set_api_creds(creds)
                logger.info(
                    "ClobClient initialised (singleton) — L2 creds supplied "
                    "via env vars (CLOB_API_KEY / CLOB_SECRET / CLOB_PASSPHRASE)"
                )
            else:
                # Fall back to deriving L2 credentials from the private key.
                # Makes one authenticated call to the CLOB; subsequent order
                # placements reuse the cached creds.
                client.set_api_creds(client.create_or_derive_api_creds())
                logger.info(
                    "ClobClient initialised (singleton) — L2 creds derived "
                    "from PRIVATE_KEY"
                )
        except Exception as e:
            logger.error("ClobClient L2 auth failed: %s", e)
            raise

        _clob_client = client
    return _clob_client

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

MAX_POSITIONS = 20  # hard cap on concurrent open positions

# ── Bounded dedup set ─────────────────────────────────────────────────────────

class BoundedSet:
    """
    A set that evicts the oldest entries once it reaches *maxsize*.

    Prevents seen_txs from growing without bound on long-running Render
    deployments where 512 MB RAM is the hard limit.  At ~64 bytes per
    tx-hash string, 10 000 entries ≈ 640 KB — negligible.
    """

    def __init__(self, maxsize: int = 10_000):
        from collections import deque
        self._maxsize = maxsize
        self._data: set[str]       = set()
        self._order: deque[str]    = deque()

    def __contains__(self, item: str) -> bool:
        return item in self._data

    def add(self, item: str) -> None:
        if item in self._data:
            return
        if len(self._data) >= self._maxsize:
            oldest = self._order.popleft()
            self._data.discard(oldest)
        self._data.add(item)
        self._order.append(item)

    def __len__(self) -> int:
        return len(self._data)


@dataclass
class BotState:
    running:         bool  = False
    total_copied:    int   = 0
    total_skipped:   int   = 0
    # market_id:outcome → {"size": usdc, "entry_price": float, "wallet_label": str}
    positions:       dict  = field(default_factory=dict)
    # tx_hash dedup — capped at 10 000 entries to avoid OOM on Render free tier
    seen_txs:        BoundedSet = field(default_factory=BoundedSet)
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
# Skip immediately on future polls instead of making a wasted HTTP call.
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
    # Data API: fully public, no auth required.
    # GET /trades?user=<address>&tAfter=<ts_seconds>&limit=50
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
    # Gamma API expects condition_id as a query param, not a path segment
    url = f"{config.POLYMARKET_GAMMA_API}/markets"
    try:
        r = await client.get(url, params={"condition_id": market_id}, timeout=10)
        r.raise_for_status()
        data = r.json()
        # Returns a list; grab the first match
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
    """Return raw order book dict with 'bids' and 'asks' lists.

    Returns an empty dict on any error.  404 responses are treated as
    permanently dead markets: the token_id is added to ``_dead_tokens``
    so subsequent polls skip the HTTP call entirely.
    """
    if token_id in _dead_tokens:
        return {}

    url = f"{config.POLYMARKET_CLOB_API}/book"
    try:
        r = await client.get(url, params={"token_id": token_id}, timeout=10)
        if r.status_code == 404:
            _dead_tokens.add(token_id)
            logger.info(
                "Order book 404 — market resolved/delisted, token cached as dead: …%s",
                token_id[-12:],
            )
            return {}
        r.raise_for_status()
        return r.json()
    except httpx.HTTPStatusError:
        # Already handled 404 above; other 4xx/5xx — log and move on
        logger.warning(
            "fetch_order_book HTTP error for token …%s: %s %s",
            token_id[-12:], r.status_code, r.reason_phrase,
        )
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

# Polymarket migrated to pUSD (Polymarket USD) on April 28 2026.
# pUSD is an ERC-20 on Polygon backed 1:1 by USDC.
PUSD_CONTRACT = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
PUSD_ABI = [{
    "inputs": [{"name": "account", "type": "address"}],
    "name": "balanceOf",
    "outputs": [{"name": "", "type": "uint256"}],
    "stateMutability": "view",
    "type": "function",
}]

def fetch_wallet_usdc_balance() -> float:
    """
    Read pUSD balance of DEPOSIT_WALLET_ADDRESS from Polygon.
    pUSD is Polymarket's native collateral token (1:1 USDC-backed) since April 28 2026.
    Falls back to the last known real_balance if the RPC call fails.
    """
    if not config.DEPOSIT_WALLET_ADDRESS:
        logger.warning("DEPOSIT_WALLET_ADDRESS not set — balance unavailable")
        return state.real_balance or 0.0
    try:
        w3 = Web3(Web3.HTTPProvider(config.POLYGON_RPC_URL, request_kwargs={"timeout": 10}))
        addr = Web3.to_checksum_address(config.DEPOSIT_WALLET_ADDRESS)
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(PUSD_CONTRACT),
            abi=PUSD_ABI,
        )
        raw = contract.functions.balanceOf(addr).call()
        balance = raw / 1_000_000   # pUSD uses 6 decimals like USDC
        if balance == 0.0:
            logger.warning(
                "pUSD balance is 0 for %s — wallet may be empty or RPC unreachable",
                config.DEPOSIT_WALLET_ADDRESS[:10],
            )
        logger.debug("Wallet pUSD balance: $%.2f", balance)
        return balance
    except Exception as e:
        logger.warning("fetch_wallet_usdc_balance (pUSD): %s — using last known balance", e)
        return state.real_balance or 0.0


# ── Trade parsing ─────────────────────────────────────────────────────────────

def parse_trade(raw: dict, wallet: str) -> Optional[PolyTrade]:
    try:
        # Data API field names (data-api.polymarket.com/trades)
        # title: market question (included in trade record — no extra API call needed)
        # slug:  market slug
        # asset: outcome token ID
        # conditionId: market/condition ID
        # size: USDC notional
        # price: fill price (0–1)
        # outcome: "Yes" / "No" / team name etc.
        # transactionHash: on-chain tx
        # timestamp: unix seconds
        side      = raw.get("side", "").upper()
        size      = float(raw.get("size", raw.get("usdcSize", 0)))
        price     = float(raw.get("price", raw.get("avgPrice", 0)))
        token_id  = raw.get("asset", raw.get("asset_id", ""))
        market_id = raw.get("conditionId", raw.get("market", raw.get("condition_id", "")))
        outcome   = raw.get("outcome", "")
        tx_hash   = raw.get("transactionHash", raw.get("transaction_hash", ""))
        # Data API returns timestamp in milliseconds — normalise to seconds.
        raw_ts    = int(raw.get("timestamp", 0))
        timestamp = raw_ts // 1000 if raw_ts > 1_000_000_000_000 else raw_ts
        title     = raw.get("title", "")
        slug      = raw.get("slug", raw.get("eventSlug", ""))

        if size < config.MIN_SOURCE_TRADE_USDC:
            return None
        if not side or not token_id or not market_id:
            logger.warning("parse_trade: missing field(s) — side=%r token_id=%r market_id=%r", side, token_id, market_id)
            return None
        if not config.COPY_EXITS and side == "SELL":
            return None
        if token_id in _dead_tokens:
            return None
        # Reject trades older than 2x the poll interval — guards against the
        # Data API returning stale history when tAfter is unreliable.
        max_age = config.POLL_INTERVAL_SEC * 2
        if timestamp and (int(time.time()) - timestamp) > max_age:
            logger.debug("parse_trade: skipping stale trade age=%ds tx=%s", int(time.time()) - timestamp, tx_hash)
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
    """Exactly 1% of current virtual balance, no floor or ceiling."""
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

    # ── LIVE ─────────────────────────────────────────────────────────────────
    # Use the module-level singleton to avoid repeated auth/key-negotiation and
    # nonce collisions.  The blocking CLOB calls are offloaded to a thread-pool
    # executor so they never stall the event loop (which would cause Render's
    # health check to time out and restart the process).
    import functools
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY as CLOB_BUY, SELL as CLOB_SELL

    # The library expects its own BUY/SELL sentinel constants, not raw strings.
    clob_side = CLOB_BUY if trade.side == "BUY" else CLOB_SELL

    clob = get_clob_client()
    order_args = OrderArgs(
        token_id=trade.token_id,
        price=limit_price,
        size=shares,
        side=clob_side,
        # order_type does NOT belong in OrderArgs — passed to post_order() below
    )
    loop = asyncio.get_running_loop()
    # Step 1: sign locally (no network).
    signed = await loop.run_in_executor(None, clob.create_order, order_args)
    # Step 2: submit to CLOB with GTC order type.
    resp = await loop.run_in_executor(
        None, functools.partial(clob.post_order, signed, OrderType.GTC)
    )

    # Normalise response: the live CLOB returns {"orderID": "...", ...} (capital ID).
    # Map to the "order_id" key the rest of the bot expects.
    if isinstance(resp, dict) and "order_id" not in resp:
        resp["order_id"] = resp.get("orderID") or resp.get("id") or "unknown"

    logger.info(
        "[LIVE] Order submitted — id=%s status=%s",
        resp.get("order_id"), resp.get("status"),
    )
    return resp
    # ─────────────────────────────────────────────────────────────────────────


async def cancel_order(client: httpx.AsyncClient, order_id: str) -> None:
    """Cancel a resting limit order by ID."""
    if config.DRY_RUN:
        logger.info("[DRY-RUN] Would cancel order %s", order_id)
        return

    # ── LIVE ─────────────────────────────────────────────────────────────────
    # Offload to executor — same reasoning as place_limit_order.
    clob = get_clob_client()
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, clob.cancel, order_id)
    logger.info("Cancelled order %s", order_id)
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
        try:
            now = int(time.time())
            expired = [
                oid for oid, o in list(state.open_orders.items())
                if now - o.placed_at >= config.LIMIT_ORDER_TTL_SEC
            ]
            for oid in expired:
                o = state.open_orders.pop(oid)
                logger.info(
                    "TTL expired — cancelling order %s (%s %s on %s)",
                    oid, o.trade.side, o.trade.outcome,
                    o.trade.market_question or o.trade.market_id,
                )
                await cancel_order(client, oid)
        except Exception as exc:
            logger.exception(
                "Unhandled error in ttl_watchdog — will retry next cycle: %s", exc
            )


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
            existing["entry_price"] = (
                existing["entry_price"] * existing["size"] + trade.price * size_usdc
            ) / total
            existing["size"]   = total
            existing["shares"] = existing.get("shares", 0) + shares
        # Deduct cost from virtual balance
        state.virtual_balance = max(0.0, state.virtual_balance - size_usdc)
        # Update peak
        total_value = state.virtual_balance + sum(p["size"] for p in state.positions.values())
        state.peak_balance = max(state.peak_balance, total_value)
    else:
        existing = state.positions.pop(key, None)
        if existing:
            entry  = existing["entry_price"]
            exit_p = trade.price
            pnl    = (exit_p - entry) * (existing["size"] / entry) if entry else 0.0

            # Credit virtual balance: return original cost + profit/loss
            proceeds = existing["size"] + pnl
            state.virtual_balance = round(state.virtual_balance + proceeds, 2)

            state.total_pnl      += pnl
            state.realised_pnl   += pnl
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

    # market_question and market_slug already populated by parse_trade from the Data API response
    if not trade.market_question:
        trade.market_question = trade.market_id[:20] + "…"

    # ── BUY guards ────────────────────────────────────────────────────────────
    if trade.side == "BUY":
        if len(state.positions) >= MAX_POSITIONS:
            logger.info(
                "Position cap reached (%d/%d) — skipping BUY %s",
                len(state.positions), MAX_POSITIONS, trade.market_question,
            )
            state.total_skipped += 1
            return

    # Fetch current order book
    book = await fetch_order_book(client, trade.token_id)
    if not book:
        if trade.token_id in _dead_tokens:
            logger.debug(
                "Skipping %s — market resolved/delisted (token …%s)",
                trade.market_question or trade.market_id[:20],
                trade.token_id[-12:],
            )
        else:
            logger.warning(
                "Empty order book for token …%s — skipping %s",
                trade.token_id[-12:],
                trade.market_question or trade.market_id[:20],
            )
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

    # ── Seed balances on startup ──────────────────────────────────────────────
    real = await asyncio.get_running_loop().run_in_executor(None, fetch_wallet_usdc_balance)
    state.real_balance    = round(real, 2)
    state.virtual_balance = round(real, 2)
    state.peak_balance    = round(real, 2)
    state.balance_seeded  = True
    logger.info(
        "Balances seeded — real: $%.2f  virtual: $%.2f",
        state.real_balance, state.virtual_balance,
    )

    # Set last_poll to NOW so we only pick up trades that happen after bot start.
    # We intentionally do NOT look back — historical trades are not copied.
    last_poll         = int(time.time())
    last_balance_poll = 0

    async with httpx.AsyncClient() as client:
        watchdog_task = asyncio.create_task(ttl_watchdog(client))
        watchdog_task.add_done_callback(_log_task_exception)

        _consecutive_errors = 0

        while state.running:
            try:
                now = int(time.time())

                # Refresh real balance every 5 minutes (non-blocking)
                if now - last_balance_poll >= 300:
                    real = await asyncio.get_running_loop().run_in_executor(
                        None, fetch_wallet_usdc_balance
                    )
                    state.real_balance = round(real, 2)
                    last_balance_poll  = now
                    logger.debug("Real balance refreshed: $%.2f", state.real_balance)

                for lbl, wallet in config.SOURCE_WALLETS.items():
                    raw_trades = await fetch_wallet_trades(client, wallet, last_poll)
                    for raw in raw_trades:
                        trade = parse_trade(raw, wallet)
                        if trade:
                            await process_trade(client, trade)

                # Refresh current prices for open positions (unrealised PnL)
                for key, pos in list(state.positions.items()):
                    tid = pos.get("token_id", "")
                    if tid and tid not in _dead_tokens:
                        book = await fetch_order_book(client, tid)
                        mid  = best_ask(book) or best_bid(book)
                        if mid:
                            pos["current_price"] = mid

                last_poll = now
                _consecutive_errors = 0          # reset on clean poll

            except Exception as exc:             # noqa: BLE001
                _consecutive_errors += 1
                # Exponential back-off: 15 s, 30 s, 60 s, 120 s … capped at 5 min
                backoff = min(300, config.POLL_INTERVAL_SEC * (2 ** min(_consecutive_errors - 1, 4)))
                logger.exception(
                    "Unhandled error in monitor_loop poll body "
                    "(consecutive=%d) — backing off %ds: %s",
                    _consecutive_errors, backoff, exc,
                )
                await asyncio.sleep(backoff)
                continue

            await asyncio.sleep(config.POLL_INTERVAL_SEC)

    logger.info("Monitor stopped.")


# ── Dashboard helpers ─────────────────────────────────────────────────────────

def get_positions_payload() -> list[dict]:
    """
    Return open positions enriched with unrealised PnL.
    Uses last known entry price as proxy for current value when no live price available.
    Live prices are fetched asynchronously by the monitor loop and stored in the position.
    """
    rows = []
    for key, pos in state.positions.items():
        entry   = pos["entry_price"]
        current = pos.get("current_price", entry)   # falls back to entry if not yet refreshed
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
        f"Poll:    every {config.POLL_INTERVAL_SEC}s",
        f"Size:    1% of wallet balance (no floor/ceiling)",
        f"Orders:  GTC limit  |  TTL: {config.LIMIT_ORDER_TTL_SEC}s  |  offset: {config.LIMIT_TICK_OFFSET}",
        f"Copied:  {state.total_copied}  |  Skipped: {state.total_skipped}",
        f"Open orders: {len(state.open_orders)}",
    ]
    if state.positions:
        lines.append("Positions:")
        for k, v in state.positions.items():
            lines.append(f"  {k}  ${v['size']:.2f} @ {v['entry_price']:.4f} [{v['wallet_label']}]")
    return "\n".join(lines)
