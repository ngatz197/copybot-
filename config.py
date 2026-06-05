"""
config.py — Polymarket Copy Trading Bot Configuration
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_IDS  = os.getenv("TELEGRAM_CHAT_IDS", "").split(",")   # comma-separated admin IDs

# ── Polymarket ───────────────────────────────────────────────────────────────
POLYMARKET_GAMMA_API  = "https://gamma-api.polymarket.com"
POLYMARKET_CLOB_API   = "https://clob.polymarket.com"

# Wallets to monitor (source wallets you want to copy)
SOURCE_WALLETS = [
    w.strip()
    for w in os.getenv("SOURCE_WALLETS", "").split(",")
    if w.strip()
]

# Your trading wallet (Polygon address)
MY_WALLET_ADDRESS  = os.getenv("MY_WALLET_ADDRESS", "")
MY_WALLET_PRIVATE_KEY = os.getenv("MY_WALLET_PRIVATE_KEY", "")   # keep this secret!

# ── Copy-trade settings ──────────────────────────────────────────────────────
# Each trade uses exactly 1% of your current USDC wallet balance
TRADE_PCT           = 0.01   # 1% — hardcoded, not configurable

# Hard min/max per order in USDC (safety guardrails around the 1%)
MIN_ORDER_USDC      = float(os.getenv("MIN_ORDER_USDC", "5"))
MAX_ORDER_USDC      = float(os.getenv("MAX_ORDER_USDC", "500"))

# Maximum slippage tolerated (fraction, e.g. 0.02 = 2%)
MAX_SLIPPAGE        = float(os.getenv("MAX_SLIPPAGE", "0.02"))

# How often to poll Polymarket activity (seconds)
POLL_INTERVAL_SEC   = int(os.getenv("POLL_INTERVAL_SEC", "15"))

# Only copy trades above this USDC threshold (ignore tiny dust trades)
MIN_SOURCE_TRADE_USDC = float(os.getenv("MIN_SOURCE_TRADE_USDC", "20"))

# Copy exits (YES → NO flips or position reductions) as well as entries
COPY_EXITS          = os.getenv("COPY_EXITS", "true").lower() == "true"

# Dry-run mode: log what WOULD be traded without actually placing orders
DRY_RUN             = os.getenv("DRY_RUN", "true").lower() == "true"

# ── Polygon RPC ──────────────────────────────────────────────────────────────
POLYGON_RPC_URL     = os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com")

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_LEVEL           = os.getenv("LOG_LEVEL", "INFO")
