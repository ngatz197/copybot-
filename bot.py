async def main():
    global _bot_ref

    # Start health server in thread
    threading.Thread(target=run_health_server, daemon=True).start()
    
    # Small delay to ensure server starts
    await asyncio.sleep(1)

    bot = CopyTrader(dry_run=DRY_RUN)
    _bot_ref = bot

    # Start WebSocket connection
    ws_task = asyncio.create_task(market_data.connect())
    
    # Small delay for WebSocket connection
    await asyncio.sleep(2)

    try:
        # Fetch initial balance
        starting = bot.balance.fetch_with_retry()
        global peak_bankroll, compounding_bankroll
        peak_bankroll = compounding_bankroll = starting if starting else INITIAL_BANKROLL
        logging.info(f"Starting balance: ${peak_bankroll:.2f}")
        
        # Run the main bot loop
        await bot.run()
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        market_data.running = False
        ws_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
