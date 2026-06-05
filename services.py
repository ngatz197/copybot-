"""
services.py — Polymarket trade monitoring & copy-trade execution
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

# ── Data models ──────────────────────────────────────────────────────────────

@dataclass
class PolyTrade:
    """A detected trade from a source wallet."""
    wallet:       str
    market_id:    str          # Polymarket condition ID
    token_id:     str          # outcome token ID (YES/NO)
    side:         str          # "BUY" or "SELL"
    size_usdc:    float        # notional in USDC
    price:        float        # 0-1 (implied probability)
    outcome:      str          # "YES" or "NO"
    market_slug:  str = ""
    market_question: str = ""
    tx_hash:      str = ""
    timestamp:    int = 0

@dataclass
class BotState:
    running:       bool = False
    paused:        bool = False
    total_copied:  int  = 0
    total_skipped: int  = 0
    total_pnl:     float = 0.0
    # market_id → {outcome: quantity} for our open positions
    positions:     dict = field(default_factory=dict)
    # set of already-processed tx hashes (dedup)
    seen_txs:      set  = field(default_factory=set)

# Singleton state shared between services and bot
state = BotState()

# ── Polymarket API helpers ────────────────────────────────────────────────────

async def fetch_wallet_trades(
    client: httpx.AsyncClient,
    wallet: str,
    since_ts: int
) -> list[dict]:
    """
    Pull recent trade activity for a wallet from the Gamma API.
    Returns raw trade dicts (newest first).
    """
    url = f"{config.POLYMARKET_GAMMA_API}/trades"
    params = {
        "maker": wallet,
        "after": since_ts,
        "limit": 50,
    }
    try:
        r = await client.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json().get("data", [])
    except Exception as e:
        logger.warning("fetch_wallet_trades(%s): %s", wallet, e)
        return []


async def fetch_market_info(client: httpx.AsyncClient, market_id: str) -> dict:
    """Fetch market metadata (question, slug, token IDs)."""
    url = f"{config.POLYMARKET_GAMMA_API}/markets/{market_id}"
    try:
        r = await client.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning("fetch_market_info(%s): %s", market_id, e)
        return {}


async def fetch_best_price(client: httpx.AsyncClient, token_id: str, side: str) -> Optional[float]:

    """
    Get current best price for a token from the CLOB order book.
    side: 'BUY' or 'SELL'
    Returns price (0-1) or None if unavailable.
    """
    url = f"{config.POLYMARKET_CLOB_API}/book"
    params = {"token_id": token_id}
    try:
        r = await client.get(url, params=params, timeout=10)
        r.raise_for_status()
        book = r.json()
        if side == "BUY":
            asks = book.get("asks", [])
            return float(asks[0]["price"]) if asks else None
        else:
            bids = book.get("bids", [])
            return float(bids[0]["price"]) if bids else None
    except Exception as e:
        logger.warning("fetch_best_price(%s %s): %s", token_id, side, e)
        return None


# ── Wallet balance ────────────────────────────────────────────────────────────

# Polymarket uses USDC on Polygon (6 decimals)
USDC_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
USDC_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

def fetch_wallet_usdc_balance() -> float:
    """
    Read current USDC balance of MY_WALLET_ADDRESS from Polygon chain.
    Returns balance in USDC (float). Falls back to MIN_ORDER_USDC on error.
    """
    if not config.MY_WALLET_ADDRESS:
        logger.warning("MY_WALLET_ADDRESS not set — using MIN_ORDER_USDC as fallback")
        return config.MIN_ORDER_USDC

    try:
        w3 = Web3(Web3.HTTPProvider(config.POLYGON_RPC_URL))
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(USDC_CONTRACT),
            abi=USDC_ABI,
        )
        raw = contract.functions.balanceOf(
            Web3.to_checksum_address(config.MY_WALLET_ADDRESS)
        ).call()
        balance = raw / 1e6   # USDC has 6 decimals
        logger.debug("Wallet USDC balance: $%.2f", balance)
        return balance
    except Exception as e:
        logger.warning("fetch_wallet_usdc_balance error: %s — falling back to MIN_ORDER_USDC", e)
        return config.MIN_ORDER_USDC


# ── Trade parsing ─────────────────────────────────────────────────────────────

def parse_trade(raw: dict, wallet: str) -> Optional[PolyTrade]:
    """
    Convert a raw Gamma API trade dict into a PolyTrade.
    Returns None if the trade should be ignored.
    """
    try:
        side      = raw.get("side", "").upper()          # BUY / SELL
        size      = float(raw.get("usdcSize", 0))
        price     = float(raw.get("price", 0))
        token_id  = raw.get("asset", "")
        market_id = raw.get("market", "")
        outcome   = raw.get("outcome", "")               # YES / NO
        tx_hash   = raw.get("transactionHash", "")
        timestamp = int(raw.get("timestamp", 0))

        if size < config.MIN_SOURCE_TRADE_USDC:
            return None
        if not side or not token_id or not market_id:
            return None
        if not config.COPY_EXITS and side == "SELL":
            return None

        return PolyTrade(
            wallet=wallet,
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
        logger.debug("parse_trade error: %s | raw=%s", e, raw)
        return None


# ── Slippage check ────────────────────────────────────────────────────────────

def slippage_ok(source_price: float, current_price: Optional[float]) -> bool:
    """Return True if current price is within MAX_SLIPPAGE of the source price."""
    if current_price is None:
        return False
    slip = abs(current_price - source_price) / max(source_price, 1e-9)
    if slip > config.MAX_SLIPPAGE:
        logger.info(
            "Slippage too high: source=%.4f current=%.4f slip=%.2f%%",
            source_price, current_price, slip * 100
        )
        return False
    return True


# ── Order sizing ──────────────────────────────────────────────────────────────

def compute_order_size() -> float:
    """
    Each trade uses exactly 1% of the current wallet USDC balance,
    clamped to MIN_ORDER_USDC / MAX_ORDER_USDC as safety guardrails.
    """
    balance = fetch_wallet_usdc_balance()
    raw = balance * config.TRADE_PCT          # 1% of balance
    size = max(config.MIN_ORDER_USDC, min(config.MAX_ORDER_USDC, raw))
    logger.info("Order size: 1%% of $%.2f balance = $%.2f", balance, size)
    return size


# ── Order execution ───────────────────────────────────────────────────────────

async def place_order(trade: PolyTrade, my_size_usdc: float, current_price: float) -> dict:
    """
    Place a market order on Polymarket CLOB.

    In production this calls the CLOB API with a signed EIP-712 order.
    The full signing flow requires py-clob-client or manual EIP-712 construction.
    This scaffold logs the intent and returns a result dict.

    To activate live trading:
      1. pip install py-clob-client
      2. Replace the stub below with ClobClient.create_and_post_order(...)
    """
    if config.DRY_RUN:
        logger.info(
            "[DRY-RUN] Would %s %.2f USDC of %s @ %.4f (market=%s)",
            trade.side, my_size_usdc, trade.outcome, current_price, trade.market_id
        )
        return {
            "status": "dry_run",
            "side": trade.side,
            "outcome": trade.outcome,
            "size_usdc": my_size_usdc,
            "price": current_price,
            "market_id": trade.market_id,
        }

    # ── LIVE ORDER (activate when DRY_RUN=false) ─────────────────────────────
    # from py_clob_client.client import ClobClient
    # from py_clob_client.clob_types import OrderArgs, OrderType
    #
    # clob = ClobClient(
    #     host=config.POLYMARKET_CLOB_API,
    #     key=config.MY_WALLET_PRIVATE_KEY,
    #     chain_id=137,
    # )
    # order_args = OrderArgs(
    #     token_id=trade.token_id,
    #     price=round(current_price, 4),
    #     size=round(my_size_usdc / current_price, 2),  # convert USDC → shares
    #     side=trade.side,
    # )
    # resp = clob.create_and_post_order(order_args)
    # return resp
    # ─────────────────────────────────────────────────────────────────────────

    raise RuntimeError("Set DRY_RUN=false and uncomment py-clob-client code to go live.")


# ── Position tracking ─────────────────────────────────────────────────────────

def update_position(trade: PolyTrade, my_size_usdc: float) -> None:
    """Track our open positions for P&L and status reporting."""
    key = f"{trade.market_id}:{trade.outcome}"
    current = state.positions.get(key, 0.0)
    if trade.side == "BUY":
        state.positions[key] = current + my_size_usdc
    else:
        state.positions[key] = max(0.0, current - my_size_usdc)
        if state.positions[key] == 0.0:
            del state.positions[key]


# ── Core copy-trade pipeline ──────────────────────────────────────────────────

async def process_trade(
    client: httpx.AsyncClient,
    trade: PolyTrade,
    notify_fn
) -> None:
    """
    Full pipeline for one detected trade:
      1. Slippage check
      2. Size computation
      3. Order placement
      4. Telegram notification
    """
    if trade.tx_hash in state.seen_txs:
        return
    state.seen_txs.add(trade.tx_hash)

    # Enrich with market metadata
    market = await fetch_market_info(client, trade.market_id)
    trade.market_question = market.get("question", trade.market_id[:16] + "…")
    trade.market_slug     = market.get("slug", "")

    # Get current best price
    current_price = await fetch_best_price(client, trade.token_id, trade.side)

    if not slippage_ok(trade.price, current_price):
        state.total_skipped += 1
        msg = (
            f"⏭ <b>Skipped</b> — slippage too high\n"
            f"Market: {trade.market_question}\n"
            f"Source: {trade.side} {trade.outcome} @ {trade.price:.4f}\n"
            f"Current: {current_price:.4f if current_price else 'N/A'}"
        )
        await notify_fn(msg)
        return

    my_size = compute_order_size()  # 1% of current wallet balance
    result  = await place_order(trade, my_size, current_price)

    update_position(trade, my_size)
    state.total_copied += 1

    emoji  = "🟢" if trade.side == "BUY" else "🔴"
    status = "DRY RUN" if config.DRY_RUN else "FILLED"
    msg = (
        f"{emoji} <b>Copy Trade — {status}</b>\n"
        f"Wallet: <code>{trade.wallet[:6]}…{trade.wallet[-4:]}</code>\n"
        f"Market: {trade.market_question}\n"
        f"Side: {trade.side} {trade.outcome}\n"
        f"My size: ${my_size:.2f} (1% of wallet balance)\n"
        f"Source price: {trade.price:.4f}  →  Fill price: {current_price:.4f}\n"
        f"Slippage: {abs(current_price - trade.price) / max(trade.price,1e-9)*100:.2f}%"
    )
    await notify_fn(msg)
    logger.info("Copied trade: %s", result)


# ── Main polling loop ─────────────────────────────────────────────────────────

async def monitor_loop(notify_fn) -> None:
    """
    Continuously poll all SOURCE_WALLETS for new trades and copy them.
    notify_fn(msg: str) is an async callback that sends a Telegram message.
    """
    logger.info("Monitor loop starting. Wallets: %s", config.SOURCE_WALLETS)
    last_poll = int(time.time()) - config.POLL_INTERVAL_SEC

    async with httpx.AsyncClient() as client:
        while state.running:
            if state.paused:
                await asyncio.sleep(5)
                continue

            now = int(time.time())

            for wallet in config.SOURCE_WALLETS:
                raw_trades = await fetch_wallet_trades(client, wallet, last_poll)
                for raw in raw_trades:
                    trade = parse_trade(raw, wallet)
                    if trade:
                        await process_trade(client, trade, notify_fn)

            last_poll = now
            await asyncio.sleep(config.POLL_INTERVAL_SEC)

    logger.info("Monitor loop stopped.")


# ── Status helpers ─────────────────────────────────────────────────────────────

def get_status_text() -> str:
    """Return a human-readable status string for the /status command."""
    status = "▶️ Running" if (state.running and not state.paused) else (
             "⏸ Paused"  if state.paused else "⏹ Stopped")
    mode   = "🔵 DRY RUN" if config.DRY_RUN else "🟠 LIVE"
    lines  = [
        f"<b>Polymarket Copy Bot</b>",
        f"Status: {status}  |  Mode: {mode}",
        f"Wallets watched: {len(config.SOURCE_WALLETS)}",
        f"Poll interval: {config.POLL_INTERVAL_SEC}s",
        f"Trade size: 1% of wallet balance  (${config.MIN_ORDER_USDC}–${config.MAX_ORDER_USDC} guardrails)",
        f"Max slippage: {config.MAX_SLIPPAGE*100:.1f}%",
        f"Trades copied: {state.total_copied}  |  Skipped: {state.total_skipped}",
    ]
    if state.positions:
        lines.append("\n<b>Open positions:</b>")
        for k, v in state.positions.items():
            lines.append(f"  {k}: ${v:.2f}")
    return "\n".join(lines)
