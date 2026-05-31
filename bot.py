import asyncio
import json
import websockets
from collections import defaultdict
import time

# ==================== CONFIG ====================
POLL_INTERVAL = 15  # now also used as fallback interval

# ==================== MARKET DATA MANAGER (WS + Fallback) ====================
class MarketDataManager:
    def __init__(self):
        self.ws = None
        self.token_to_price: Dict[str, float] = {}          # current mid / last price
        self.token_to_source_positions: Dict[str, list] = defaultdict(list)  # for source positions
        self.subscribed_tokens: Set[str] = set()
        self.last_fallback = 0
        self.reconnect_attempts = 0
        self.running = False

    async def connect(self):
        """Main WebSocket loop with fallback"""
        self.running = True
        uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

        while self.running:
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=30) as ws:
                    self.ws = ws
                    self.reconnect_attempts = 0
                    logging.info("✅ WebSocket connected to Polymarket Market Channel")

                    # Initial subscription for already known tokens
                    if self.subscribed_tokens:
                        await self._subscribe(list(self.subscribed_tokens))

                    async for message in ws:
                        try:
                            data = json.loads(message)
                            await self._handle_ws_message(data)
                        except Exception as e:
                            logging.warning(f"WS message parse error: {e}")

            except Exception as e:
                self.reconnect_attempts += 1
                delay = min(2 ** self.reconnect_attempts, 30)
                logging.warning(f"WebSocket disconnected: {e} — reconnecting in {delay}s")
                await asyncio.sleep(delay)

            # Fallback polling every 15s when WS is down
            if time.time() - self.last_fallback > 15:
                await self._fallback_poll()
                self.last_fallback = time.time()

    async def _subscribe(self, token_ids: list):
        if not self.ws or not token_ids:
            return
        msg = {
            "assets_ids": token_ids,
            "type": "market",
            "custom_feature_enabled": True
        }
        await self.ws.send(json.dumps(msg))
        self.subscribed_tokens.update(token_ids)
        logging.info(f"WS subscribed to {len(token_ids)} new token(s)")

    async def _handle_ws_message(self, data: dict):
        event_type = data.get("event_type")
        asset_id = data.get("asset_id")

        if not asset_id:
            return

        if event_type == "book":
            # Full orderbook snapshot
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            best_bid = float(bids[0]["price"]) if bids else 0.0
            best_ask = float(asks[0]["price"]) if asks else 0.0
            mid = (best_bid + best_ask) / 2 if best_bid and best_ask else best_ask or best_bid
            self.token_to_price[asset_id] = round(mid, 6)

        elif event_type == "price_change" or event_type == "last_trade_price":
            price = data.get("price") or data.get("last_trade_price")
            if price:
                self.token_to_price[asset_id] = round(float(price), 6)

    async def _fallback_poll(self):
        """Original API polling as fallback"""
        logging.debug("Using API poll fallback (15s)")

    def get_current_price(self, token_id: str) -> float:
        return self.token_to_price.get(token_id, 0.0)

    async def update_subscriptions(self, new_tokens: Set[str]):
        to_sub = new_tokens - self.subscribed_tokens
        if to_sub and self.ws:
            await self._subscribe(list(to_sub))

# Global instance
market_data = MarketDataManager()


# ==================== COPY TRADER – MODIFIED PARTS ONLY ====================
class CopyTrader:
    def __init__(self, dry_run: bool = True):
        # ... existing init ...
        self.market_data = market_data   # attach

    async def scan_and_copy(self):
        global current_bankroll, compounding_bankroll, bot_paused_until

        if bot_paused_until and datetime.now() < bot_paused_until:
            # ... same ...

        if self.check_drawdown():
            return

        current_bankroll = self.balance.get_balance()
        if current_bankroll is None:
            return

        logging.info(f"Scanning | WS active: {self.market_data.ws is not None} | ...")

        source_token_ids_by_wallet: Dict[str, set] = {}

        for wallet_addr, config in WALLETS.items():
            # ... same logic until position loop ...

            for pos in raw:
                token_id = pos.get("asset", "")
                # ... existing filtering ...

                # Update current price from WebSocket (preferred) or fallback
                cur_price = self.market_data.get_current_price(token_id)
                if cur_price <= 0:
                    cur_price = float(pos.get("curPrice", 0))   # fallback to REST

                # ... rest of buy logic unchanged ...

            source_token_ids_by_wallet[wallet_addr] = source_token_ids

            # Update prices for open positions (WS preferred)
            for _pk, _pos in self.positions.items():
                if _pos.source_wallet == wallet_addr:
                    ws_price = self.market_data.get_current_price(_pos.token_id)
                    if ws_price > 0:
                        _pos.current_price = ws_price
                    elif float(pos.get("curPrice", 0)) > 0:   # from this poll
                        _pos.current_price = float(pos.get("curPrice", 0))

            # SELL LOGIC remains 100% unchanged ...

        self._process_pending_orders(source_token_ids_by_wallet)

        # Dynamic WS subscription for new tokens
        all_active_tokens = {p.token_id for p in self.positions.values()} | \
                            {p.token_id for p in self.pending.values()}
        await self.market_data.update_subscriptions(all_active_tokens)

    # ... all other methods unchanged ...


# ==================== ENTRY POINT ====================
async def main():
    global _bot_ref

    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()

    bot = CopyTrader(dry_run=DRY_RUN)
    _bot_ref = bot

    # Start WebSocket in background
    ws_task = asyncio.create_task(market_data.connect())

    try:
        starting_balance = bot.balance.fetch_with_retry(retries=5, delay=10)
        # ... same ...
        await bot.run()
    finally:
        market_data.running = False
        ws_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())
