import math
import os
from datetime import datetime, timezone
from typing import Any

import yfinance as yf

from ml.jobs._base import BaseJob


class FetchQuotesJob(BaseJob):
    # The Kafka message {"job":"fetch_quotes"} maps here.
    job_name = "fetch_quotes"

    def execute(self, payload: dict[str, Any], idempotency_key: str = "") -> list[dict[str, Any]]:
        # One scheduler trigger can ask for one ticker or many.
        tickers = parse_tickers(payload)
        quotes: list[dict[str, Any]] = []
        failures: list[str] = []

        for ticker in tickers:
            try:
                # DEMO_MODE is useful when the rest of the pipeline should run without Yahoo.
                quote = demo_quote(ticker) if is_demo_mode(payload) else fetch_yfinance_quote(ticker)

                # The DB helper writes the quote and returns the row idempotency key.
                row_key = self.db.upsert_quote(
                    ticker=quote["ticker"],
                    price=quote["price"],
                    timestamp=quote["time"],
                    volume=quote.get("volume"),
                    bid=quote.get("bid"),
                    ask=quote.get("ask"),
                    source=quote["source"],
                )
                quote["idempotency_key"] = row_key
                quote["user_id"] = self.user_id

                # Redis is for "latest quote now".
                # Timescale is for "quote history later".
                self.redis.cache_and_publish_quote(quote["ticker"], quote)
                quotes.append(quote)
                self.logger.info("stored quote ticker=%s price=%s", quote["ticker"], quote["price"])
            except Exception as exc:
                # Keep going so one bad ticker does not kill the whole batch.
                failures.append(f"{ticker}: {exc}")
                self.logger.exception("failed to fetch quote ticker=%s", ticker)

        if not quotes:
            # If nothing worked, fail the whole job so Kafka can retry it.
            raise RuntimeError("all quote fetches failed: " + "; ".join(failures))

        if failures:
            # Partial failure is still useful to log.
            self.logger.warning("partial quote fetch failure failures=%s", failures)
        return quotes


def run(payload: dict[str, Any] | None = None, idempotency_key: str = "") -> list[dict[str, Any]]:
    # Module-level entrypoint used by ml.worker.
    return FetchQuotesJob().run(payload, idempotency_key=idempotency_key)


def parse_tickers(payload: dict[str, Any]) -> list[str]:
    # Priority:
    # 1. Kafka payload tickers
    # 2. Kafka payload ticker
    # 3. TICKERS env
    # 4. default AAPL
    raw = payload.get("tickers") or payload.get("ticker") or os.getenv("TICKERS", "AAPL")
    if isinstance(raw, str):
        parts = raw.split(",")
    elif isinstance(raw, list):
        parts = raw
    else:
        raise ValueError(f"tickers must be a string or list, got {type(raw).__name__}")

    tickers = [str(part).strip().upper() for part in parts if str(part).strip()]
    if not tickers:
        raise ValueError("no tickers configured")
    return tickers


def is_demo_mode(payload: dict[str, Any]) -> bool:
    # This lets the same job run in real mode or fake-data mode.
    raw = payload.get("demo_mode", os.getenv("DEMO_MODE", "false"))
    return str(raw).lower() in {"1", "true", "yes", "on"}


def fetch_yfinance_quote(ticker: str) -> dict[str, Any]:
    # This is the real network fetch path.
    ticker = ticker.upper()
    yf_ticker = yf.Ticker(ticker)
    fast_info = yf_ticker.fast_info

    # Try the fast path first.
    price = safe_float(first_fast_info(fast_info, "last_price", "lastPrice"))
    volume = safe_int(first_fast_info(fast_info, "last_volume", "lastVolume"))
    bid = safe_float(read_fast_info(fast_info, "bid"))
    ask = safe_float(read_fast_info(fast_info, "ask"))
    timestamp = datetime.now(timezone.utc)

    # Some tickers do not expose fast_info reliably.
    # If that happens, fall back to the latest 1-minute candle.
    if price is None:
        history = yf_ticker.history(period="1d", interval="1m", prepost=True)
        if history.empty:
            raise RuntimeError(f"yfinance returned no price data for {ticker}")

        clean = history.dropna(subset=["Close"])
        if clean.empty:
            raise RuntimeError(f"yfinance returned no close prices for {ticker}")

        latest = clean.iloc[-1]
        price = safe_float(latest.get("Close"))
        volume = volume or safe_int(latest.get("Volume"))
        timestamp = latest.name.to_pydatetime()

    if price is None:
        raise RuntimeError(f"missing price for {ticker}")

    # Keep one stable quote shape for DB, Redis, and logs.
    return {
        "ticker": ticker,
        "price": price,
        "volume": volume,
        "bid": bid,
        "ask": ask,
        "time": timestamp,
        "source": "yfinance",
    }


def demo_quote(ticker: str) -> dict[str, Any]:
    # Fake quote path for demos and offline pipeline tests.
    ticker = ticker.upper()
    base_prices = {
        "AAPL": 210.0,
        "MSFT": 475.0,
        "NVDA": 145.0,
    }
    base = base_prices.get(ticker, 100.0)
    minute = datetime.now(timezone.utc).minute
    price = round(base + (minute % 7) * 0.13, 2)
    return {
        "ticker": ticker,
        "price": price,
        "volume": 1000 + minute,
        "bid": round(price - 0.01, 2),
        "ask": round(price + 0.01, 2),
        "time": datetime.now(timezone.utc),
        "source": "demo",
    }


def read_fast_info(fast_info: Any, key: str) -> Any:
    # yfinance objects are not always plain dicts.
    try:
        return fast_info.get(key)
    except AttributeError:
        return getattr(fast_info, key, None)


def first_fast_info(fast_info: Any, *keys: str) -> Any:
    # yfinance changed some field names across versions, so we try both forms.
    for key in keys:
        value = read_fast_info(fast_info, key)
        if value is not None:
            return value
    return None


def safe_float(value: Any) -> float | None:
    # Convert anything numeric-like into float, otherwise return None.
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out):
        return None
    return out


def safe_int(value: Any) -> int | None:
    # Same idea as safe_float, but for whole numbers like volume.
    if value is None:
        return None
    try:
        out = int(value)
    except (TypeError, ValueError):
        return None
    return out if out >= 0 else None


if __name__ == "__main__":
    # Handy local entrypoint:
    # python -m ml.jobs.fetch_quotes
    logging_configured = bool(os.getenv("LOG_LEVEL"))
    if not logging_configured:
        import logging

        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run()
