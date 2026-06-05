"""
bot.py — Telegram bot interface for Polymarket Copy Trading Bot
"""

import asyncio
import logging
import sys

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

import config
import services
from services import state

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ── Auth guard ────────────────────────────────────────────────────────────────

def is_authorised(update: Update) -> bool:
    uid = str(update.effective_user.id)
    return uid in config.TELEGRAM_CHAT_IDS


async def deny(update: Update) -> None:
    await update.message.reply_text("⛔ Unauthorised.")


# ── Telegram notify callback ──────────────────────────────────────────────────

async def notify(msg: str) -> None:
    """Send a message to all admin chat IDs."""
    app = _app  # set in main()
    for cid in config.TELEGRAM_CHAT_IDS:
        try:
            await app.bot.send_message(
                chat_id=cid.strip(),
                text=msg,
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning("notify(%s): %s", cid, e)


# ── Command handlers ──────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorised(update):
        return await deny(update)

    if state.running:
        await update.message.reply_text("⚠️ Bot is already running.")
        return

    state.running = True
    state.paused  = False

    mode = "🔵 DRY RUN" if config.DRY_RUN else "🟠 LIVE"
    await update.message.reply_text(
        f"✅ <b>Copy bot started</b>  [{mode}]\n"
        f"Watching {len(config.SOURCE_WALLETS)} wallet(s)\n"
        f"Poll interval: {config.POLL_INTERVAL_SEC}s",
        parse_mode=ParseMode.HTML,
    )

    # Kick off the monitor loop as a background task
    asyncio.create_task(services.monitor_loop(notify))


async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorised(update):
        return await deny(update)

    if not state.running:
        await update.message.reply_text("ℹ️ Bot is not running.")
        return

    state.running = False
    state.paused  = False
    await update.message.reply_text("⏹ <b>Bot stopped.</b>", parse_mode=ParseMode.HTML)


async def cmd_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorised(update):
        return await deny(update)

    if not state.running:
        await update.message.reply_text("ℹ️ Bot is not running.")
        return

    state.paused = not state.paused
    label = "⏸ Paused" if state.paused else "▶️ Resumed"
    await update.message.reply_text(f"{label}.", parse_mode=ParseMode.HTML)


async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorised(update):
        return await deny(update)

    await update.message.reply_text(
        services.get_status_text(),
        parse_mode=ParseMode.HTML,
    )


async def cmd_wallets(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorised(update):
        return await deny(update)

    if not config.SOURCE_WALLETS:
        await update.message.reply_text("No source wallets configured.")
        return

    lines = ["<b>Watched wallets:</b>"]
    for i, w in enumerate(config.SOURCE_WALLETS, 1):
        lines.append(f"{i}. <code>{w}</code>")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_positions(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_authorised(update):
        return await deny(update)

    if not state.positions:
        await update.message.reply_text("No open positions.")
        return

    lines = ["<b>Open positions:</b>"]
    for k, v in state.positions.items():
        market_id, outcome = k.split(":", 1)
        lines.append(f"• {outcome} on <code>{market_id[:12]}…</code>  → <b>${v:.2f}</b>")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "<b>Polymarket Copy Bot — Commands</b>\n\n"
        "/start      — Start monitoring &amp; copying trades\n"
        "/stop       — Stop the bot\n"
        "/pause      — Toggle pause (keeps loop alive, skips trades)\n"
        "/status     — Show runtime stats &amp; config\n"
        "/wallets    — List watched source wallets\n"
        "/positions  — Show open copy positions\n"
        "/help       — This message\n\n"
        "Set <code>DRY_RUN=false</code> in env to enable live trading."
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


# ── App bootstrap ─────────────────────────────────────────────────────────────

_app: Application = None  # module-level reference for notify()


def main() -> None:
    global _app

    if not config.TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN is not set. Exiting.")
        sys.exit(1)

    if not config.SOURCE_WALLETS:
        logger.warning("No SOURCE_WALLETS configured — bot will run but find nothing to copy.")

    app = Application.builder().token(config.TELEGRAM_BOT_TOKEN).build()
    _app = app

    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("stop",      cmd_stop))
    app.add_handler(CommandHandler("pause",     cmd_pause))
    app.add_handler(CommandHandler("status",    cmd_status))
    app.add_handler(CommandHandler("wallets",   cmd_wallets))
    app.add_handler(CommandHandler("positions", cmd_positions))
    app.add_handler(CommandHandler("help",      cmd_help))

    logger.info("Bot polling started.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
