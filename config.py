"""
config.py — Polymarket Copy Trading Bot Configuration
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Polymarket ───────────────────────────────────────────────────────────────
POLYMARKET_GAMMA_API  = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_API   = "https://clob.polymarket.com"

# ── Source wallets to monitor ────────────────────────────────────────────────
SOURCE_WALLETS = {
    "RN":    "0x2005d16a84ceefa912d4e380cd32e7ff827875ea",
    "Kruto": "0xe8ca3f758c93f44f3ec210542ab78afb7c0bcccb",
    "Viser": "0xf903c4cd098184e67a06a04f9b8fdb36e7bbe028",
}

# ── Your trading wallet (Polygon) ────────────────────────────────────────────
DEPOSIT_WALLET_ADDRESS     = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")  # keep secret!

# ── Trade sizing ─────────────────────────────────────────────────────────────
TRADE_PCT        = 0.01    # 1% of wallet balance per trade — hardcoded
MIN_ORDER_USDC   = float(os.getenv("MIN_ORDER_USDC", "5"))
MAX_ORDER_USDC   = float(os.getenv("MAX_ORDER_USDC", "500"))

# ── Limit order settings ─────────────────────────────────────────────────────
# Price offset applied to limit orders to ensure fast fills while avoiding
# market-order slippage. BUY limits placed this many ticks above best ask;
# SELL limits placed this many ticks below best bid.
LIMIT_TICK_OFFSET = float(os.getenv("LIMIT_TICK_OFFSET", "0.001"))  # ~0.1c

# Cancel and replace if unfilled after this many seconds (0 = never cancel)
LIMIT_ORDER_TTL_SEC = int(os.getenv("LIMIT_ORDER_TTL_SEC", "60"))

# ── Polling ───────────────────────────────────────────────────────────────────
POLL_INTERVAL_SEC     = int(os.getenv("POLL_INTERVAL_SEC", "15"))
MIN_SOURCE_TRADE_USDC = float(os.getenv("MIN_SOURCE_TRADE_USDC", "20"))
COPY_EXITS            = os.getenv("COPY_EXITS", "true").lower() == "true"

# ── Safety ────────────────────────────────────────────────────────────────────
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

# ── Polygon RPC ───────────────────────────────────────────────────────────────
POLYGON_RPC_URL = os.getenv("POLYGON_RPC_URL", "https://polygon-bor-rpc.publicnode.com")

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
