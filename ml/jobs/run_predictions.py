import logging
import os
from datetime import datetime, timezone
from typing import Any

import yfinance as yf

from ml.jobs._base import BaseJob
from ml.models import ensemble


DEFAULT_HISTORY_PERIOD = "90d"
DEFAULT_HISTORY_INTERVAL = "1d"


class RunPredictionsJob(BaseJob):
    # Kafka message {"job":"run_predictions"} maps here.
    job_name = "run_predictions"

    def execute(self, payload: dict[str, Any], idempotency_key: str = "") -> list[dict[str, Any]]:
        # Keep the same payload/env style as fetch_quotes and compute_indicators.
        tickers = parse_tickers(payload)
        period = str(payload.get("period") or os.getenv("PREDICTION_HISTORY_PERIOD", DEFAULT_HISTORY_PERIOD))
        interval = str(payload.get("interval") or os.getenv("PREDICTION_HISTORY_INTERVAL", DEFAULT_HISTORY_INTERVAL))
        horizon_days = int(payload.get("horizon_days") or os.getenv("PREDICTION_HORIZON_DAYS", "7"))

        predictions: list[dict[str, Any]] = []
        failures: list[str] = []

        for ticker in tickers:
            try:
                # Fetch history here, then keep modeling hidden behind the ensemble interface.
                history = fetch_history(ticker, period=period, interval=interval)
                prediction = ensemble.predict(ticker=ticker, history=history, horizon_days=horizon_days)
                row_key = self.db.upsert_prediction(prediction, timestamp=datetime.now(timezone.utc))
                prediction["idempotency_key"] = row_key
                prediction["user_id"] = self.user_id
                predictions.append(prediction)
                self.logger.info(
                    "stored prediction ticker=%s direction=%s confidence=%s",
                    prediction["ticker"],
                    prediction["direction"],
                    prediction["confidence"],
                )
            except Exception as exc:
                # A single bad ticker should not block predictions for the rest of the batch.
                failures.append(f"{ticker}: {exc}")
                self.logger.exception("failed to run prediction ticker=%s", ticker)

        if not predictions:
            raise RuntimeError("all predictions failed: " + "; ".join(failures))

        if failures:
            self.logger.warning("partial prediction failure failures=%s", failures)
        return predictions


def run(payload: dict[str, Any] | None = None, idempotency_key: str = "") -> list[dict[str, Any]]:
    # Module-level entrypoint used by ml.worker.
    return RunPredictionsJob().run(payload, idempotency_key=idempotency_key)


def parse_tickers(payload: dict[str, Any]) -> list[str]:
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


def fetch_history(ticker: str, period: str = DEFAULT_HISTORY_PERIOD, interval: str = DEFAULT_HISTORY_INTERVAL) -> Any:
    # 90d of daily bars gives Prophet enough recent closes without making the first baseline slow.
    frame = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=False)
    if frame.empty:
        raise RuntimeError(f"yfinance returned no history for {ticker}")
    return flatten_yfinance_columns(frame).dropna(subset=["Close"])


def flatten_yfinance_columns(frame: Any) -> Any:
    # yfinance sometimes returns a multi-index column shape for one ticker too.
    if getattr(frame.columns, "nlevels", 1) > 1:
        frame = frame.copy()
        frame.columns = [str(col[0]) for col in frame.columns]
    return frame


if __name__ == "__main__":
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run()
