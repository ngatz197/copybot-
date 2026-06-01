#!/usr/bin/env python3
"""
services.py - Infrastructure & Service Layer
- Handles Postgres DB / Local File deduplication engine.
- Manages Robust multichain JSON-RPC balance parsers for pUSD.
- Wraps py-clob-client-v2 off-chain execution calls for placing and canceling trades.
"""

import time
import json
import requests
import logging
from typing import Tuple, Set, Optional
import config as cfg

# ==================== CLOB V2 DEPENDENCY LOADER ====================
try:
    from py_clob_client_v2 import (
        ClobClient,
        OrderArgs,
        MarketOrderArgs,
        OrderType,
        Side,
        ApiCreds,
        PartialCreateOrderOptions,
    )
    CLOB_AVAILABLE = True
except ImportError:
    CLOB_AVAILABLE = False

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False


class SeenTradesStore:
    def __init__(self, filepath: str, db_url: str = ""):
        self.filepath = filepath
        self.db_url   = db_url
        self._seen: Set[str] = set()
        self._conn   = None
        self.backend = "local-file"

        if db_url and PSYCOPG2_AVAILABLE:
            self._init_postgres()
        else:
            self._load_file()

        logging.info(f"SeenTradesStore ready | backend={self.backend} | {len(self._seen)} keys loaded")

    def _init_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require")
            self._conn.autocommit = True
            with self._conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS seen_trades (
                        pos_key    TEXT PRIMARY KEY,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
            self._seen   = self._load_postgres()
            self.backend = "postgres"
        except Exception as e:
            logging.error(f"Postgres init failed: {e} — falling back to local file")
            self._conn = None
            self._load_file()

    def _load_postgres(self) -> Set[str]:
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT pos_key FROM seen_trades")
                return {row[0] for row in cur.fetchall()}
        except Exception as e:
            logging.warning(f"Postgres load failed: {e}")
            return set()

    def _save_postgres(self, pos_key: str):
        try:
            with self._conn.cursor() as cur:
                cur.execute("INSERT INTO seen_trades (pos_key) VALUES (%s) ON CONFLICT DO NOTHING", (pos_key,))
        except Exception as e:
            logging.warning(f"Postgres save failed: {e}")
            self._reconnect_postgres()

    def _save_postgres_many(self, keys):
        if not keys: return
        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, "INSERT INTO seen_trades (pos_key) VALUES %s ON CONFLICT DO NOTHING", [(k,) for k in keys])
        except Exception as e:
            logging.warning(f"Postgres bulk save failed: {e}")
            self._reconnect_postgres()

    def _reconnect_postgres(self):
        try:
            self._conn = psycopg2.connect(self.db_url, sslmode="require")
            self._conn.autocommit = True
        except Exception as e:
            logging.error(f"Postgres reconnect failed: {e}")

    def _load_file(self):
        try:
            with open(self.filepath, "r") as f:
                data = json.load(f)
                self._seen = set(data) if isinstance(data, list) else set()
        except FileNotFoundError:
            self._seen = set()
        except Exception as e:
            logging.warning(f"Could not read seen trades file: {e}")
            self._seen = set()

    def _save_file(self):
        try:
            with open(self.filepath, "w") as f:
                json.dump(sorted(self._seen), f)
        except Exception as e:
            logging.warning(f"Could not save seen trades file: {e}")

    def is_seen(self, pos_key: str) -> bool:
        return pos_key in self._seen

    def mark_seen(self, pos_key: str):
        if pos_key in self._seen: return
        self._seen.add(pos_key)
        if self._conn:
            self._save_postgres(pos_key)
        else:
            self._save_file()

    def snapshot_existing(self, pos_keys):
        new_keys = [k for k in pos_keys if k not in self._seen]
        if not new_keys: return
        for k in new_keys:
            self._seen.add(k)
        if self._conn:
            self._save_postgres_many(new_keys)
        else:
            self._save_file()
        logging.info(f"Snapshot: marked {len(new_keys)} pre-existing trades as seen")


class RobustBalanceManager:
    POLYGON_RPCS = [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com",
        "https://polygon.drpc.org",
    ]

    def __init__(self):
        self.cached_balance: Optional[float] = None
        self.last_update    = 0

    def _fetch_balance(self) -> float:
        if not cfg.YOUR_WALLET:
            return 0.0
        padded  = cfg.YOUR_WALLET.lower().replace("0x", "").zfill(64)
        payload = {
            "jsonrpc": "2.0",
            "method":  "eth_call",
            "params":  [{"to": cfg.PUSD_CONTRACT_ADDRESS, "data": "0x70a08231" + padded}, "latest"],
            "id": 1,
        }
        for rpc in self.POLYGON_RPCS:
            try:
                resp = requests.post(rpc, json=payload, timeout=8)
                if resp.status_code == 200:
                    result = resp.json().get("result", "0x0")
                    if result and result not in ("0x", "0x0"):
                        return int(result, 16) / 1_000_000
            except Exception:
                continue
        return 0.0

    def get_balance(self, force=False) -> Optional[float]:
        if force or self.cached_balance is None or (time.time() - self.last_update > 30):
            real = self._fetch_balance()
            if real > 0:
                self.cached_balance = real
                self.last_update    = time.time()
                if real > cfg.peak_bankroll:
                    cfg.peak_bankroll = real
        return self.cached_balance

    def fetch_with_retry(self, retries: int = 5, delay: int = 10) -> float:
        for attempt in range(1, retries + 1):
            val = self._fetch_balance()
            if val > 0:
                self.cached_balance = val
                self.last_update    = time.time()
                return val
            time.sleep(delay)
        raise RuntimeError("Could not fetch real pUSD balance.")


class PolymarketExecutor:
    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.client  = None

        if not dry_run and CLOB_AVAILABLE and cfg.YOUR_PRIVATE_KEY:
            try:
                creds = ApiCreds(
                    api_key        = cfg.POLY_API_KEY,
                    api_secret     = cfg.POLY_SECRET,
                    api_passphrase = cfg.POLY_PASSPHRASE,
                )
                self.client = ClobClient(
                    host     = "https://clob.polymarket.com",
                    chain_id = 137,
                    key      = cfg.YOUR_PRIVATE_KEY,
                    creds    = creds,
                )
                logging.info("ClobClient V2 execution engine online")
            except Exception as e:
                logging.error(f"ClobClient V2 init failed: {e}")

    def place_limit_buy(self, token_id: str, amount_usd: float, limit_price: float) -> Tuple[bool, str, float]:
        shares = round(amount_usd / limit_price, 4)
        if self.dry_run or self.client is None:
            return True, "dry-run-limit-buy", limit_price

        for attempt in range(cfg.MAX_RETRIES):
            try:
                result = self.client.create_and_post_order(
                    order_args = OrderArgs(token_id=token_id, price=limit_price, size=shares, side=Side.BUY),
                    options    = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.GTC,
                )
                return True, result.get("orderID", result.get("id", "unknown")), limit_price
            except Exception as e:
                time.sleep(cfg.RETRY_DELAY)
        return False, "", limit_price

    def cancel_order(self, order_id: str) -> bool:
        if self.dry_run or self.client is None: return True
        try:
            self.client.cancel(order_id)
            return True
        except Exception:
            return False

    def is_order_filled(self, order_id: str) -> bool:
        if self.dry_run or self.client is None: return True
        try:
            status = self.client.get_order(order_id).get("status", "").lower()
            return status in ("matched", "filled")
        except Exception:
            return False

    def place_sell(self, token_id: str, shares: float) -> Tuple[bool, str]:
        if self.dry_run or self.client is None: return True, "dry-run-sell"
        for attempt in range(cfg.MAX_RETRIES):
            try:
                result = self.client.create_and_post_market_order(
                    order_args = MarketOrderArgs(token_id=token_id, amount=shares, side=Side.SELL),
                    options    = PartialCreateOrderOptions(tick_size="0.01"),
                    order_type = OrderType.FOK,
                )
                return True, result.get("orderID", result.get("id", "unknown"))
            except Exception:
                time.sleep(cfg.RETRY_DELAY)
        return False, ""