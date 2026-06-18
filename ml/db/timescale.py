import hashlib
import os
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row


class TimescaleClient(AbstractContextManager["TimescaleClient"]):
    def __init__(self, user_id: str | None = None) -> None:
        self.user_id = user_id or os.getenv("USER_ID", "default")
        self._conn: psycopg.Connection[dict[str, Any]] | None = None

    def __enter__(self) -> "TimescaleClient":
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def connect(self) -> psycopg.Connection[dict[str, Any]]:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(conninfo(), row_factory=dict_row)
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    def upsert_quote(
        self,
        ticker: str,
        price: float,
        timestamp: datetime,
        volume: int | None = None,
        bid: float | None = None,
        ask: float | None = None,
        source: str = "yfinance",
    ) -> str:
        timestamp = ensure_utc(timestamp)
        key = quote_idempotency_key(self.user_id, ticker, timestamp)
        self.execute(
            """
            INSERT INTO market_quotes
                (time, user_id, ticker, price, volume, bid, ask, source, idempotency_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, idempotency_key) DO NOTHING
            """,
            (timestamp, self.user_id, ticker.upper(), price, volume, bid, ask, source, key),
        )
        return key

    def upsert_ohlcv(
        self,
        ticker: str,
        timestamp: datetime,
        open_price: float | None,
        high: float | None,
        low: float | None,
        close: float | None,
        volume: int | None,
        interval: str = "1d",
    ) -> str:
        timestamp = ensure_utc(timestamp)
        key = ohlcv_idempotency_key(self.user_id, ticker, timestamp, interval)
        self.execute(
            """
            INSERT INTO ohlcv
                (time, user_id, ticker, open, high, low, close, volume, interval, idempotency_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, idempotency_key) DO NOTHING
            """,
            (timestamp, self.user_id, ticker.upper(), open_price, high, low, close, volume, interval, key),
        )
        return key

    def upsert_indicators(self, ticker: str, timestamp: datetime, indicators: dict[str, Any]) -> str:
        timestamp = ensure_utc(timestamp)
        key = indicator_idempotency_key(self.user_id, ticker, timestamp)
        self.execute(
            """
            INSERT INTO indicators
                (time, user_id, ticker, rsi_14, macd, macd_signal, macd_hist,
                 bb_upper, bb_middle, bb_lower, atr_14, obv, ema_20, sma_50, idempotency_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, idempotency_key) DO NOTHING
            """,
            (
                timestamp,
                self.user_id,
                ticker.upper(),
                indicators.get("rsi_14"),
                indicators.get("macd"),
                indicators.get("macd_signal"),
                indicators.get("macd_hist"),
                indicators.get("bb_upper"),
                indicators.get("bb_middle"),
                indicators.get("bb_lower"),
                indicators.get("atr_14"),
                indicators.get("obv"),
                indicators.get("ema_20"),
                indicators.get("sma_50"),
                key,
            ),
        )
        return key


def conninfo() -> str:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "marketintel")
    user = os.getenv("DB_USER", "marketintel")
    password = os.getenv("DB_PASSWORD", "marketintel_dev")
    return f"host={host} port={port} dbname={name} user={user} password={password}"


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def hash_key(*parts: object) -> str:
    content = ":".join(str(part) for part in parts)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def minute_bucket(value: datetime) -> str:
    value = ensure_utc(value)
    return value.strftime("%Y-%m-%dT%H:%M")


def quote_idempotency_key(user_id: str, ticker: str, timestamp: datetime) -> str:
    return hash_key(user_id, ticker.upper(), minute_bucket(timestamp))


def ohlcv_idempotency_key(user_id: str, ticker: str, timestamp: datetime, interval: str) -> str:
    return hash_key(user_id, ticker.upper(), interval, minute_bucket(timestamp))


def indicator_idempotency_key(user_id: str, ticker: str, timestamp: datetime) -> str:
    return hash_key(user_id, ticker.upper(), minute_bucket(timestamp))

