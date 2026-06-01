async def scan_and_copy(self):
        if cfg.bot_paused_until and datetime.now() < cfg.bot_paused_until: return
        if self.check_drawdown(): return

        current_bal = self.balance.get_balance()
        if current_bal is None: return

        logging.info(f"Scan Run | Net Assets: ${current_bal:.2f} pUSD | Active Orders: {len(self.positions)} | Open Limits: {len(self.pending)}")
        source_token_ids_by_wallet = {}

        for wallet_addr, config in cfg.WALLETS.items():
            raw = self._get_positions(wallet_addr)
            if raw is None: 
                logging.warning(f"[{config['name']}] Failed to fetch positions from API.")
                continue

            source_token_ids = {pos.get("asset") for pos in raw if pos.get("asset") and float(pos.get("size", pos.get("shares", 0))) > 0}

            # =================================================================
            # VERBOSE LOGGING: This prints the summary for each individual wallet
            # =================================================================
            logging.info(f"[{config['name']}] {len(raw)} position(s) from API, {len(source_token_ids)} with active tokens")

            # =================================================================
            # VERBOSE LOGGING: This loops and prints EVERY position line-by-line
            # =================================================================
            for pos in raw:
                token_id  = pos.get("asset", "")
                question  = pos.get("title", "Unknown")
                size_usd  = float(pos.get("currentValue", 0))
                pos_key   = f"{wallet_addr}_{token_id}"
                
                is_seen   = self.seen.is_seen(pos_key)
                is_open   = pos_key in self.positions
                is_pending = pos_key in self.pending
                
                logging.info(f"[{config['name']}] {question[:40]}... | seen={is_seen} open={is_open} pending={is_pending} val=${size_usd:.2f}")

            # First scan logic to snapshot existing positions safely
            if wallet_addr not in self._first_scan_done:
                self._first_scan_done.add(wallet_addr)
                if config.get("copy_mode") == "new_only":
                    self.seen.snapshot_existing({f"{wallet_addr}_{tid}" for tid in source_token_ids})
                    source_token_ids_by_wallet[wallet_addr] = source_token_ids
                    continue

            # Core processing logic for order copying
            for pos in raw:
                token_id  = pos.get("asset", "")
                market_id = pos.get("conditionId", "")
                question  = pos.get("title", "Unknown")
                outcome   = pos.get("outcome", "YES")
                size_usd  = float(pos.get("currentValue", 0))

                min_val = 0.0 if config.get("copy_sub_dollar") else 1.0
                if not token_id or size_usd < min_val or size_usd <= 0: continue

                pos_key = f"{wallet_addr}_{token_id}"
                if self.seen.is_seen(pos_key) or pos_key in self.positions or pos_key in self.pending: continue
                if len(self.positions) + len(self.pending) >= cfg.MAX_POSITIONS: break

                cur_price = float(pos.get("curPrice", 0))
                if cur_price <= 0: continue

                limit_price = round(cur_price, 4)
                if config.get("copy_sub_dollar") and size_usd < 1.0:
                    my_size = round(size_usd, 2)
                else:
                    my_size = round(cfg.compounding_bankroll * self.get_risk_percent(limit_price, config), 2)

                ok, order_id, actual_price = self.executor.place_limit_buy(token_id, my_size, limit_price)
                if ok:
                    self.seen.mark_seen(pos_key)
                    self.pending[pos_key] = PendingLimitBuy(
                        pos_key=pos_key, token_id=token_id, market_id=market_id, question=question,
                        outcome=outcome, source_wallet=wallet_addr, source_name=config["name"],
                        limit_price=actual_price, size_usd=my_size, order_id=order_id
                    )

            source_token_ids_by_wallet[wallet_addr] = source_token_ids

            cur_price_map = {pos.get("asset"): float(pos.get("curPrice", 0)) for pos in raw if pos.get("asset") and float(pos.get("curPrice", 0)) > 0}
            for _pk, _pos in self.positions.items():
                if _pos.source_wallet == wallet_addr and _pos.token_id in cur_price_map:
                    _pos.current_price = cur_price_map[_pos.token_id]

            for pos_key, position in list(self.positions.items()):
                if position.source_wallet != wallet_addr: continue
                if position.token_id not in source_token_ids and position.status == "open":
                    exit_price, _ = self.get_orderbook_prices(position.token_id)
                    ok, _ = self.executor.place_sell(position.token_id, position.shares)
                    if ok:
                        pnl = (exit_price - position.entry_price) * position.shares
                        position.status, position.exit_price, position.pnl = "closed", exit_price, pnl
                        if pnl > 0:
                            cfg.compounding_bankroll += pnl * cfg.COMPOUNDING_RATE
                        self.closed_positions.append(position)
                        del self.positions[pos_key]

        self._process_pending_orders(source_token_ids_by_wallet)