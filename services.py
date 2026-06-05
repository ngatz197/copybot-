"""
bot.py — Polymarket Copy Trading Bot entry point
Runs the trade monitor loop AND the FastAPI dashboard server concurrently.
"""

import asyncio
import logging
import os
import signal
import sys
import time
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

import config
import services
from services import state, _wallet_stats

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Dashboard server port — Render sets PORT automatically
PORT = int(os.getenv("PORT", "8080"))

DASHBOARD_PATH = Path(__file__).parent / "dashboard.html"

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="Polymarket CopyTrader")


@app.get("/", response_class=HTMLResponse)
async def index():
    return DASHBOARD_PATH.read_text()


@app.get("/data")
async def data():
    positions_payload = services.get_positions_payload()
    unrealised_pnl    = round(sum(p["unrealised_pnl"] for p in positions_payload), 4)
    realised_pnl      = round(state.realised_pnl, 4)
    total_pnl         = round(realised_pnl + unrealised_pnl, 4)

    # Drawdown: (peak - current_total) / peak * 100
    current_total = state.virtual_balance + sum(p["size"] for p in state.positions.values())
    peak          = state.peak_balance if state.peak_balance > 0 else current_total
    drawdown      = round((peak - current_total) / peak * 100, 1) if peak > 0 else 0.0

    total_wins   = sum(ws.wins   for ws in state.wallet_stats.values())
    total_losses = sum(ws.losses for ws in state.wallet_stats.values())
    total_closed = total_wins + total_losses
    win_rate     = round(total_wins / total_closed * 100, 1) if total_closed else 0.0

    wallets_payload = {}
    for label in config.SOURCE_WALLETS:
        ws     = _wallet_stats(label)
        closed = ws.wins + ws.losses
        wallets_payload[label] = {
            "total_pnl": ws.total_pnl,
            "win_rate":  round(ws.wins / closed * 100, 1) if closed else 0.0,
            "wins":      ws.wins,
            "losses":    ws.losses,
            "open":      ws.open,
            "closed_trades": [
                {
                    "outcome":     ct.outcome,
                    "entry_price": ct.entry_price,
                    "exit_price":  ct.exit_price,
                    "pnl":         ct.pnl,
                }
                for ct in ws.closed_trades
            ],
        }

    return JSONResponse({
        "dry_run":         config.DRY_RUN,
        "real_balance":    state.real_balance,
        "virtual_balance": round(state.virtual_balance, 2),
        "peak_balance":    round(state.peak_balance, 2),
        "total_pnl":       total_pnl,
        "unrealised_pnl":  unrealised_pnl,
        "realised_pnl":    realised_pnl,
        "drawdown":        drawdown,
        "win_rate":        win_rate,
        "open_count":      len(state.positions),
        "max_positions":   services.MAX_POSITIONS,
        "pnl_history":     state.pnl_history[-100:],
        "positions":       positions_payload,
        "wallets":         wallets_payload,
        "total_closed":    total_closed,
    })


# ── Bot coroutines ────────────────────────────────────────────────────────────

def handle_shutdown(sig, frame):
    logger.info("Signal %s received — shutting down…", sig)
    state.running = False


async def status_printer() -> None:
    """Print a status summary every 5 minutes."""
    while state.running:
        await asyncio.sleep(300)
        if state.running:
            logger.info("\n%s", services.get_status_text())


async def run_webserver() -> None:
    """Run uvicorn in-process so it shares the same event loop."""
    cfg = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level=config.LOG_LEVEL.lower(),
        access_log=False,
    )
    server = uvicorn.Server(cfg)
    await server.serve()


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    if not config.DEPOSIT_WALLET_ADDRESS:
        logger.error("DEPOSIT_WALLET_ADDRESS is not set. Exiting.")
        sys.exit(1)

    if not config.PRIVATE_KEY and not config.DRY_RUN:
        logger.error("PRIVATE_KEY is not set and DRY_RUN=false. Exiting.")
        sys.exit(1)

    mode = "DRY RUN" if config.DRY_RUN else "*** LIVE TRADING ***"
    logger.info("=" * 60)
    logger.info("Polymarket Copy Bot starting — %s", mode)
    logger.info("Watching %d wallets:", len(config.SOURCE_WALLETS))
    for lbl, addr in config.SOURCE_WALLETS.items():
        logger.info("  %-8s %s", lbl, addr)
    logger.info("Trade size : 1%% of wallet balance ($%.0f–$%.0f guardrails)",
                config.MIN_ORDER_USDC, config.MAX_ORDER_USDC)
    logger.info("Order type : GTC limit  |  tick offset: %.4f  |  TTL: %ds",
                config.LIMIT_TICK_OFFSET, config.LIMIT_ORDER_TTL_SEC)
    logger.info("Poll       : every %ds", config.POLL_INTERVAL_SEC)
    logger.info("Dashboard  : http://0.0.0.0:%d", PORT)
    logger.info("=" * 60)

    state.running = True

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT,  handle_shutdown)

    await asyncio.gather(
        services.monitor_loop(),
        status_printer(),
        run_webserver(),
    )

    logger.info("Bot stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
