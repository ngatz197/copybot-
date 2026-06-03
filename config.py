import os
import logging
from datetime import datetime
from typing import Optional, Dict
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ==================== OPERATIONAL FLAGS ====================
DRY_RUN          = os.getenv("DRY_RUN", "true").lower() == "true"
POLL_INTERVAL    = int(os.getenv("POLL_SECONDS", "40"))
COMPOUNDING_RATE = float(os.getenv("COMPOUNDING_RATE", "0.70"))  # ✅ fixed typo
MAX_DRAWDOWN     = float(os.getenv("MAX_DRAWDOWN", "0.20"))
HEALTH_PORT      = int(os.getenv("PORT", "8080"))

# ==================== WALLET DEFINITIONS ====================
WALLETS: Dict[str, dict] = {
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
        "copy_mode": "new_only",
    },
}

# ==================== SECRETS & REQS ====================
YOUR_PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
YOUR_WALLET      = os.getenv("DEPOSIT_WALLET_ADDRESS", "")
POLY_API_KEY     = os.getenv("POLY_API_KEY", "")
POLY_SECRET      = os.getenv("POLY_SECRET", "")
POLY_PASSPHRASE  = os.getenv("POLY_PASSPHRASE", "")
DATABASE_URL     = os.getenv("DATABASE_URL", "")

INITIAL_BANKROLL      = 0.0   # sentinel — overwritten by fetch_with_retry() at startup
MAX_POSITIONS         = int(os.getenv("MAX_POSITIONS", "8"))
PAUSE_HOURS           = 48
MAX_RETRIES           = 3
RETRY_DELAY           = 5

LIMIT_BUY_MAX_PREMIUM   = float(os.getenv("LIMIT_BUY_MAX_PREMIUM", "0.20"))
LIMIT_EXPIRY_SECONDS    = int(os.getenv("LIMIT_EXPIRY_SECONDS", "300"))
SEEN_TRADES_FILE        = os.getenv("SEEN_TRADES_FILE", "seen_trades.json")
PUSD_CONTRACT_ADDRESS   = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"

# Minimum fractional reduction in source-wallet shares that triggers a partial
# copy-sell on our side.  Default 0.20 = copy any sell ≥ 20 % of their position.
PARTIAL_SELL_THRESHOLD  = float(os.getenv("PARTIAL_SELL_THRESHOLD", "0.20"))

# When placing a limit sell, we post at best_bid.  This is the maximum discount
# we will accept below mid-price (e.g. 0.05 = will not sell more than 5 % below
# mid).  If best_bid is worse than this floor we fall back to mid × (1 - discount).
SELL_LIMIT_MAX_DISCOUNT = float(os.getenv("SELL_LIMIT_MAX_DISCOUNT", "0.05"))

# ==================== RUNTIME SYSTEM HOOKS ====================
bot_paused_until:     Optional[datetime] = None
compounding_bankroll: float = 0.0   # set to real balance at startup via fetch_with_retry()
peak_bankroll:        float = 0.0   # same — 0.0 means "not yet initialised"
_bot_ref                    = None  # Dashboard context anchor
