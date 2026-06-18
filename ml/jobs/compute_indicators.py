import logging
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yfinance as yf

from ml.jobs._base import BaseJob


# The compiled extension is written to the repo root by scripts/build_indicators.sh.
# Add that path explicitly so this job works when run as `python -m ml.jobs.compute_indicators`.
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import indicators  # noqa: E402


class ComputeIndicatorsJob(BaseJob):
    job_name = "compute_indicators"

    def execute(self, payload: dict[str, Any], idempotency_key: str = "") -> list[dict[str, Any]]:
        # Payload values can override env defaults, which keeps the scheduler flexible.
        tickers = parse_tickers(payload)
        period = str(payload.get("period") or os.getenv("INDICATOR_HISTORY_PERIOD", "6mo"))
        interval = str(payload.get("interval") or os.getenv("INDICATOR_HISTORY_INTERVAL", "1d"))

        results: list[dict[str, Any]] = []
        failures: list[str] = []

        for ticker in tickers:
            try:
                # Step 1: download enough OHLCV history for indicator math.
                frame = fetch_history(ticker, period=period, interval=interval)
                # Step 2: compute one latest indicator snapshot from that history.
                row_time, values = compute_latest(frame)
                # Step 3: persist the snapshot. The DB client owns idempotency details.
                row_key = self.db.upsert_indicators(ticker, row_time, values)
                result = {
                    "ticker": ticker,
                    "time": row_time,
                    "idempotency_key": row_key,
                    **values,
                }
                results.append(result)
                self.logger.info("stored indicators ticker=%s rsi_14=%s", ticker, values.get("rsi_14"))
            except Exception as exc:
                failures.append(f"{ticker}: {exc}")
                self.logger.exception("failed to compute indicators ticker=%s", ticker)

        if not results:
            raise RuntimeError("all indicator computations failed: " + "; ".join(failures))

        if failures:
            self.logger.warning("partial indicator computation failure failures=%s", failures)
        return results


def run(payload: dict[str, Any] | None = None, idempotency_key: str = "") -> list[dict[str, Any]]:
    return ComputeIndicatorsJob().run(payload, idempotency_key=idempotency_key)


def parse_tickers(payload: dict[str, Any]) -> list[str]:
    # Accept one ticker, many tickers, or an env fallback for local runs.
    raw = payload.get("tickers") or payload.get("ticker") or os.getenv("TICKERS", "AAPL,NVDA,MSFT")
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


def fetch_history(ticker: str, period: str = "6mo", interval: str = "1d") -> Any:
    # Remote I/O stays here. Everything after this point is local computation.
    frame = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=False)
    if frame.empty:
        raise RuntimeError(f"yfinance returned no history for {ticker}")
    return flatten_yfinance_columns(frame).dropna(subset=["Close"])


def compute_latest(frame: Any) -> tuple[datetime, dict[str, float | None]]:
    # The native module wants plain float arrays, not pandas Series objects.
    closes = numeric_series(frame, "Close")
    highs = numeric_series(frame, "High")
    lows = numeric_series(frame, "Low")
    volumes = numeric_series(frame, "Volume")

    if len(closes) < 50:
        raise ValueError("at least 50 historical rows are required for the Sprint 3 indicators")

    macd_result = indicators.macd(closes)
    bands = indicators.bollinger(closes, 20, 2.0)

    # Store only the newest values. We do not persist the full indicator series in this sprint.
    values = {
        "rsi_14": latest_number(indicators.rsi(closes, 14)),
        "macd": latest_number(macd_result.macd),
        "macd_signal": latest_number(macd_result.signal),
        "macd_hist": latest_number(macd_result.histogram),
        "bb_upper": latest_number(bands.upper),
        "bb_middle": latest_number(bands.middle),
        "bb_lower": latest_number(bands.lower),
        "atr_14": latest_number(indicators.atr(highs, lows, closes, 14)),
        "obv": latest_number(indicators.obv(closes, volumes)),
        "ema_20": latest_number(indicators.ema(closes, 20)),
        "sma_50": latest_number(indicators.sma(closes, 50)),
    }
    return latest_timestamp(frame), values


def flatten_yfinance_columns(frame: Any) -> Any:
    # yfinance sometimes returns a multi-index column shape for one ticker too.
    if getattr(frame.columns, "nlevels", 1) > 1:
        frame = frame.copy()
        frame.columns = [str(col[0]) for col in frame.columns]
    return frame


def numeric_series(frame: Any, column: str) -> list[float]:
    # Fail loudly if upstream columns are missing so we do not compute garbage.
    if column not in frame:
        raise ValueError(f"history is missing {column}")
    return [float(value) for value in frame[column].dropna().to_numpy().ravel().tolist()]


def latest_timestamp(frame: Any) -> datetime:
    # Use the market bar timestamp, not the job runtime, for indicator storage.
    latest = frame.index[-1]
    if hasattr(latest, "to_pydatetime"):
        value = latest.to_pydatetime()
    else:
        value = datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def latest_number(values: list[float]) -> float | None:
    # Most indicators begin with NaN warmup values. Walk backward to the newest real number.
    for value in reversed(values):
        number = float(value)
        if not math.isnan(number):
            return number
    return None


if __name__ == "__main__":
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run()
