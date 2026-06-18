import hashlib
import os
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


class TimescaleClient(AbstractContextManager["TimescaleClient"]):
    def __init__(self, user_id: str | None = None) -> None:
        # Every row is tagged with one user id.
        self.user_id = user_id or os.getenv("USER_ID", "default")
        self._conn: psycopg.Connection[dict[str, Any]] | None = None

    def __enter__(self) -> "TimescaleClient":
        self.connect()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def connect(self) -> psycopg.Connection[dict[str, Any]]:
        # Open lazily so jobs only connect if they really need the DB.
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(conninfo(), row_factory=dict_row)
        return self._conn

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        # Small helper for write queries.
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        # Small helper for read queries that return many rows.
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall())

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        # Small helper for read queries that return one row.
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
        # Normalize time first so every writer uses UTC.
        timestamp = ensure_utc(timestamp)
        # The idempotency key is what keeps repeated writes from duplicating the same quote.
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

    def upsert_prediction(self, prediction: dict[str, Any], timestamp: datetime | None = None) -> str:
        # One row stores the latest forecast snapshot for one ticker/model/horizon.
        timestamp = minute_timestamp(timestamp or datetime.now(timezone.utc))
        ticker = str(prediction["ticker"]).upper()
        model = str(prediction["model"])
        horizon_days = int(prediction["horizon_days"])
        key = prediction_idempotency_key(self.user_id, ticker, model, horizon_days, timestamp)
        self.execute(
            """
            INSERT INTO predictions
                (time, user_id, ticker, model, horizon_days, direction, confidence,
                 forecast_values, feature_importance, idempotency_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, idempotency_key) DO NOTHING
            """,
            (
                timestamp,
                self.user_id,
                ticker,
                model,
                horizon_days,
                prediction["direction"],
                float(prediction["confidence"]),
                Jsonb(prediction.get("forecast_values")),
                Jsonb(prediction["feature_importance"]) if prediction.get("feature_importance") is not None else None,
                key,
            ),
        )
        return key


def conninfo() -> str:
    # Read DB settings from env so Docker and local runs share one code path.
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "marketintel")
    user = os.getenv("DB_USER", "marketintel")
    password = os.getenv("DB_PASSWORD", "marketintel_dev")
    return f"host={host} port={port} dbname={name} user={user} password={password}"


def ensure_utc(value: datetime) -> datetime:
    # We store all times in UTC so comparisons stay clean.
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def hash_key(*parts: object) -> str:
    # Short stable hash used for idempotency keys.
    content = ":".join(str(part) for part in parts)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def minute_bucket(value: datetime) -> str:
    # Sprint 2 quote jobs run on minute-level schedule, so minute granularity is enough.
    value = ensure_utc(value)
    return value.strftime("%Y-%m-%dT%H:%M")


def minute_timestamp(value: datetime) -> datetime:
    # Use the same minute bucket in the stored time and the idempotency hash.
    value = ensure_utc(value)
    return value.replace(second=0, microsecond=0)


def quote_idempotency_key(user_id: str, ticker: str, timestamp: datetime) -> str:
    # Same user + ticker + minute => same logical quote write.
    return hash_key(user_id, ticker.upper(), minute_bucket(timestamp))


def ohlcv_idempotency_key(user_id: str, ticker: str, timestamp: datetime, interval: str) -> str:
    # Interval matters for OHLCV because 1m and 1d bars are different rows.
    return hash_key(user_id, ticker.upper(), interval, minute_bucket(timestamp))


def indicator_idempotency_key(user_id: str, ticker: str, timestamp: datetime) -> str:
    return hash_key(user_id, ticker.upper(), minute_bucket(timestamp))


def prediction_idempotency_key(
    user_id: str,
    ticker: str,
    model: str,
    horizon_days: int,
    timestamp: datetime,
) -> str:
    return hash_key(user_id, ticker.upper(), model, horizon_days, minute_bucket(timestamp))
