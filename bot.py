# ==================== BALANCE LAYER ====================
class RobustBalanceManage:
    """
    Manages cached balances, tracks total portfolio value, and acts
    as a safeguard for available capital constraints.
    """
    def __init__(self, initial_bankroll: float):
        self.cached_balance: float = initial_bankroll
        self.last_fetched: float = 0.0
        self.lock = asyncio.Lock()

    def update_balance(self, amount: float):
        self.cached_balance = round(float(amount), 4)
        self.last_fetched = time.monotonic()


# ==================== CORE COPY TRADER BOT ====================
class MultiWalletCopyTrader:
    def __init__(self):
        self.dry_run: bool = DRY_RUN
        self.balance = RobustBalanceManage(INITIAL_BANKROLL)
        
        # State Management Structures
        self.positions: Dict[str, Position] = {}               # Key: token_id (Open Positions)
        self.closed_positions: List[Position] = []            # Historical realized trades
        self.pending_buys: Dict[str, PendingLimitBuy] = {}     # Key: pos_key (Active limit orders)
        
        # Performance Circuit Breakers and Cache Layers
        self.clob_breaker = CircuitBreaker("Polymarket_CLOB")
        self.pos_cache = PositionCache()
        self.book_cache = OrderbookCache()
        
        # Specialized EIP-712 Matrix Cache Setup
        self.signer_account = Account.from_key(YOUR_PRIVATE_KEY) if YOUR_PRIVATE_KEY else None
        
        # DB / Local Storage Fallback Layer
        self.store = SeenTradesStore(SEEN_TRADES_FILE, DATABASE_URL)
        
        # Core Orchestration Clients
        self.client: Optional[ClobClient] = None
        if CLOB_AVAILABLE and YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(
                    api_key=POLY_API_KEY,
                    secret=POLY_SECRET,
                    passphrase=POLY_PASSPHRASE
                )
                self.client = ClobClient(
                    host="https://clob.polymarket.com",
                    key=YOUR_PRIVATE_KEY,
                    creds=creds
                )
                logging.info("⚡ Polymarket ClobClient initialized successfully.")
            except Exception as e:
                logging.error(f"❌ Critical error initializing ClobClient: {e}")

    def _available_balance(self) -> float:
        """Calculates liquid capital excluding funds tied up in pending limit orders."""
        reserved = sum(pb.size_usd for pb in self.pending_buys.values())
        return max(0.0, self.balance.cached_balance - reserved)

    # ------------------------------------------------------------------------
    # HIGH PERFORMANCE NETWORK PIPELINE & CRYPTO PRE-SIGNING
    # ------------------------------------------------------------------------
    async def get_pre_warmed_session(self) -> aiohttp.ClientSession:
        """Retrieves or spins up a global keep-alive network pipeline connection."""
        global WARM_HTTP_SESSION
        if WARM_HTTP_SESSION is None or WARM_HTTP_SESSION.closed:
            connector = aiohttp.TCPConnector(keepalive_timeout=60, ttl_dns_cache=300)
            WARM_HTTP_SESSION = aiohttp.ClientSession(connector=connector)
            logging.info("🔥 Pre-warmed persistent HTTP socket connection pipeline established.")
        return WARM_HTTP_SESSION

    def pre_sign_order_matrix(self, token_id: str, side: Side, price: float, size: float) -> dict:
        """
        Caches and signs raw EIP-712 structured payloads directly in memory (RAM).
        Eliminates downstream JSON serialization/signing overhead during critical HFT execution path.
        """
        if not self.signer_account:
            return {}
            
        matrix_key = f"{token_id}_{side}_{price}_{size}"
        if matrix_key in PRE_SIGNED_MATRIX_CACHE:
            return PRE_SIGNED_MATRIX_CACHE[matrix_key]

        # Structure abstract EIP-712 Polymarket payload parameters
        domain_data = {
            "name": "ClobMarket",
            "version": "1",
            "chainId": 137, # Polygon Mainnet
            "verifyingContract": PUSD_CONTRACT_ADDRESS
        }
        
        message_types = {
            "Order": [
                {"name": "token", "type": "address"},
                {"name": "side", "type": "uint8"},
                {"name": "price", "type": "uint256"},
                {"name": "amount", "type": "uint256"},
                {"name": "signer", "type": "address"}
            ]
        }
        
        message_payload = {
            "token": token_id,
            "side": 0 if side == Side.BUY else 1,
            "price": int(price * 1e6), # Fixed 6-decimal scaling
            "amount": int(size * 1e6),
            "signer": self.signer_account.address
        }

        try:
            signable_data = encode_typed_data(domain_data, message_types, message_payload)
            signed_msg = self.signer_account.sign_message(signable_data)
            
            payload = {
                "message": message_payload,
                "signature": signed_msg.signature.hex()
            }
            PRE_SIGNED_MATRIX_CACHE[matrix_key] = payload
            return payload
        except Exception as e:
            logging.error(f"Failed to compile pre-signed matrix: {e}")
            return {}

    # ------------------------------------------------------------------------
    # ORIGINAL TIERED SCALING AND RISK ALLOCATION STRUCTS
    # ------------------------------------------------------------------------
    def calculate_risk_allocation(self, wallet_config: dict, entry_price: float) -> float:
        """
        RESTORED: Multi-tier dynamic risk allocation scaling rules.
        - price_based: Original Tiered Rule -> 3% if < $0.15, 1% if < $0.65, else 0.6%
        - fixed: Constant parameter matching original constant constraints (e.g., 2.5%)
        """
        global compounding_bankroll
        
        if wallet_config["risk_type"] == "fixed":
            allocation_ratio = wallet_config.get("fixed_risk", 0.025)
        else:
            # Re-implemented Original Multi-Tier Sizing Rules Exactly
            if entry_price < 0.15:
                allocation_ratio = 0.030  # 3.0% allocation tier
            elif entry_price < 0.65:
                allocation_ratio = 0.010  # 1.0% allocation tier
            else:
                allocation_ratio = 0.006  # 0.6% allocation tier

        raw_size = compounding_bankroll * allocation_ratio
        
        # Sub-dollar execution checks
        if entry_price < 1.0 and not wallet_config.get("copy_sub_dollar", True):
            return 0.0
            
        return round(raw_size, 2)

    # ------------------------------------------------------------------------
    # BULLETPROOF INITIALIZATION SAFEGUARDS
    # ------------------------------------------------------------------------
    async def bootstrap_historical_state(self):
        """
        Failsafe State Resolver: Syncs existing on-chain metrics on boot or recovery.
        Intercepts and flags pre-existing active trades to avoid infinite execution copy-loops 
        caused by unexpected socket or infrastructure disconnects.
        """
        logging.info("🔄 Initiating systemic bootstrap sequence...")
        
        # Populate initial tracking configurations dynamically via standard pipelines
        session = await self.get_pre_warmed_session()
        await throttle.acquire()
        
        # If the local or cloud datastore is empty, snapshot current states to establish baseline telemetry
        if self.store.is_empty:
            logging.info("Empty data tracking structure detected. Snapshotting current profile configuration...")
            baseline_keys = []
            
            # Scrape upstream endpoints to extract historical execution tracks
            for target_wallet in WALLETS.keys():
                positions_list = await self.fetch_upstream_positions(target_wallet, session)
                for pos in positions_list:
                    # Construct structural transaction execution tracking signatures
                    pos_key = f"{target_wallet}_{pos['asset_id']}_{pos['side']}"
                    baseline_keys.append(pos_key)
                    
            if baseline_keys:
                self.store.snapshot_existing(baseline_keys)
        
        # Dynamically evaluate portfolio balance layers
        await self.sync_account_balances(session)
        logging.info("✅ System historical initialization state verified complete.")

    async def fetch_upstream_positions(self, wallet: str, session: aiohttp.ClientSession) -> list:
        """Fetches active open positions for a target wallet from the remote API."""
        if not self.clob_breaker.allow_request():
            cached = self.pos_cache.get(wallet)
            return cached if cached is not None else []

        url = f"https://clob.polymarket.com/positions?user={wallet}"
        try:
            async with session.get(url, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    positions = data if isinstance(data, list) else []
                    self.pos_cache.set(wallet, positions)
                    self.clob_breaker.record_success()
                    return positions
                else:
                    self.clob_breaker.record_failure()
        except Exception as e:
            logging.error(f"Error fetching upstream positions for {wallet}: {e}")
            self.clob_breaker.record_failure()
        
        cached = self.pos_cache.get(wallet)
        return cached if cached is not None else []

    async def sync_account_balances(self, session: aiohttp.ClientSession):
        """Refreshes system equity layers and recalculates the compounding sizing bases."""
        global current_bankroll, peak_bankroll, compounding_bankroll
        
        if self.dry_run:
            self.balance.update_balance(current_bankroll)
            return

        url = f"https://clob.polymarket.com/balance?user={YOUR_WALLET}"
        try:
            async with session.get(url, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    balance_val = float(data.get("balance", current_bankroll))
                    self.balance.update_balance(balance_val)
                    current_bankroll = balance_val
                    
                    if current_bankroll > peak_bankroll:
                        peak_bankroll = current_bankroll
                        
                    # Recalculate our compounding sizing baseline based on defined compounding rules
                    compounding_bankroll = INITIAL_BANKROLL + ((current_bankroll - INITIAL_BANKROLL) * COMPOUNDING_RATE)
        except Exception as e:
            logging.error(f"Failed to sync real-time balances: {e}")

    # ------------------------------------------------------------------------
    # MARKET EXECUTION ENGINE WITH ANTI-SLIPPAGE SHIELDS
    # ------------------------------------------------------------------------
    async def process_wallet_scan(self):
        """Orchestrates high-frequency iterative scans across monitored target wallets."""
        global bot_paused_until
        if bot_paused_until and datetime.now() < bot_paused_until:
            return

        session = await self.get_pre_warmed_session()
        
        # Parallelize data collection over persistent sockets
        tasks = [self.fetch_upstream_positions(w, session) for w in WALLETS.keys()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, target_wallet in enumerate(WALLETS.keys()):
            res = results[i]
            if isinstance(res, Exception) or not res:
                continue
                
            wallet_config = WALLETS[target_wallet]
            
            for pos in res:
                asset_id = pos["asset_id"]
                side_str = pos.get("side", "BUY").upper()
                shares_val = float(pos.get("size", 0.0))
                price_val = float(pos.get("avgPrice", 0.0))
                
                if shares_val <= 0 or price_val <= 0:
                    continue

                pos_key = f"{target_wallet}_{asset_id}_{side_str}"
                
                # Copy mode routing filters
                if wallet_config["copy_mode"] == "new_only" and self.store.is_seen(pos_key):
                    continue

                # Process position discovery rules
                if not self.store.is_seen(pos_key):
                    with _trade_lock:
                        if len(self.positions) >= wallet_config.get("max_positions", MAX_POSITIONS):
                            logging.warning(f"⚠️ Position cap hit for allocation channel: {wallet_config['name']}")
                            continue
                    
                    await self.execute_copy_order(target_wallet, wallet_config, asset_id, side_str, price_val, shares_val, pos_key)

    async def execute_copy_order(self, wallet_addr: str, config: dict, asset_id: str, side: str, upstream_price: float, upstream_shares: float, pos_key: str):
        """
        Calculates position limits, applies hard slippage limits, 
        evaluates EIP-712 pre-signed payloads, and submits transactions.
        """
        if side != "BUY":
            return # Sells are managed explicitly via trailing portfolio reconciliation engines

        allocated_usd = self.calculate_risk_allocation(config, upstream_price)
        if allocated_usd <= 0 or allocated_usd > self._available_balance():
            return

        # STIGHT LIMIT PREMIUM SHIELD EVALUATION
        max_premium = config.get("limit_buy_max_premium", LIMIT_BUY_MAX_PREMIUM)
        execution_limit_price = round(upstream_price * (1.0 + max_premium), 2)
        if execution_limit_price > 0.99:
            execution_limit_price = 0.99 # Hard boundary protocol cap

        # Optimize orders: Prefetch RAM payloads or dispatch via persistent connections
        target_shares = round(allocated_usd / upstream_price, 4)
        
        logging.info(f"🚀 Execution triggered for [{config['name']}] -> Asset: {asset_id} | Target Price: {upstream_price} | Shield Cap: {execution_limit_price}")

        if self.dry_run:
            # Instantly update local data profiles during dry runs
            self.store.mark_seen(pos_key)
            mock_pos = Position(
                market_id="mock_mkt", question="Simulation Mirror Position Contract",
                outcome="YES", token_id=asset_id, entry_price=upstream_price,
                size_usd=allocated_usd, shares=target_shares, source_wallet=wallet_addr,
                source_name=config["name"]
            )
            with _trade_lock:
                self.positions[asset_id] = mock_pos
            logging.info(f"🟢 [DRY RUN] Position successfully mirrored: {asset_id}")
            return

        # Live Execution via ClobClient API
        if self.client:
            await throttle.acquire()
            try:
                # Pre-compile the typed data signature matrix payload to minimize order execution latency
                matrix_payload = self.pre_sign_order_matrix(asset_id, Side.BUY, execution_limit_price, target_shares)
                
                order_args = OrderArgs(
                    price=str(execution_limit_price),
                    amount=str(target_shares),
                    tokenId=asset_id,
                    side=Side.BUY
                )
                
                # Execute the order directly using the pre-compiled high-performance payload matrices
                loop = asyncio.get_event_loop()
                resp = await loop.run_in_executor(
                    None, 
                    lambda: self.client.create_order(order_args, PartialCreateOrderOptions(signature=matrix_payload.get("signature")))
                )
                
                if resp and resp.get("success"):
                    self.store.mark_seen(pos_key)
                    # Track limit structures dynamically inside the Pending engine
                    self.pending_buys[pos_key] = PendingLimitBuy(
                        pos_key=pos_key, token_id=asset_id, market_id="live_mkt",
                        question="Live Polymarket Contract", outcome="YES",
                        source_wallet=wallet_addr, source_name=config["name"],
                        limit_price=execution_limit_price, size_usd=allocated_usd,
                        order_id=resp.get("orderID", "")
                    )
                    logging.info(f"✅ Live execution order submitted to Polymarket: {resp.get('orderID')}")
            except Exception as e:
                logging.error(f"❌ Critical failure routing live order payload matrix: {e}")

    # ------------------------------------------------------------------------
    # REAL-TIME EVENT ENGINE LOOP
    # ------------------------------------------------------------------------
    async def runtime_loop(self):
        """Asynchronous execution container managing system scans and event signals."""
        await self.bootstrap_historical_state()
        
        logging.info("🏁 Core engines activated. Streaming high-frequency portfolio channels...")
        while True:
            try:
                # Execution channels step forward based on either scheduled intervals or WS event triggers
                await self.process_wallet_scan()
                
                # Fallback sweep timeout mechanism
                try:
                    await asyncio.wait_for(market_data.activity_event.wait(), timeout=POLL_INTERVAL)
                    market_data.activity_event.clear()
                except asyncio.TimeoutError:
                    pass # Standard fallback poll interval expiration
                    
            except Exception as e:
                logging.error(f"Error in main runtime loop cycle: {e}")
                await asyncio.sleep(2)


# ==================== MAIN APPLICATION SCRIPT ====================
async def main():
    global _bot_ref
    
    # 1. Initialize the primary Multi-Wallet Engine
    bot = MultiWalletCopyTrader()
    _bot_ref = bot
    
    # 2. Spin up the background web visual monitoring engine
    server_thread = threading.Thread(target=run_health_server, daemon=True)
    server_thread.start()
    
    # 3. Establish asynchronous WebSocket data listener layers
    market_data.running = True
    asyncio.create_task(market_data.connect_market())
    asyncio.create_task(market_data.connect_user(list(WALLETS.keys())))
    
    # 4. Attach token matrices dynamically based on configured parameters
    # (In production, these auto-update when wallet profiles adapt)
    
    # 5. Execute the runtime engine loop
    await bot.runtime_loop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down Multi-Wallet Copy Trader safely...")
