import math
from datetime import date, datetime
from typing import Any

import pandas as pd
from prophet import Prophet


DEFAULT_HORIZON_DAYS = 7
DEFAULT_MIN_HISTORY_ROWS = 30
DEFAULT_NEUTRAL_THRESHOLD_PCT = 0.01


def predict_close(
    ticker: str,
    history: Any,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    neutral_threshold_pct: float = DEFAULT_NEUTRAL_THRESHOLD_PCT,
) -> dict[str, Any]:
    """Train on daily closes and return the stable Sprint 4 prediction contract."""
    # Keep one strict contract here so jobs, DB writes, and later APIs all speak the same shape.
    ticker = ticker.strip().upper()
    if not ticker:
        raise ValueError("ticker is required")
    if horizon_days <= 0:
        raise ValueError("horizon_days must be positive")

    prophet_frame = prepare_prophet_frame(history)
    if len(prophet_frame) < DEFAULT_MIN_HISTORY_ROWS:
        raise ValueError(f"at least {DEFAULT_MIN_HISTORY_ROWS} history rows are required for Prophet")

    # The latest close is our anchor for turning the forecast into up/down/neutral.
    latest_close = float(prophet_frame["y"].iloc[-1])

    # Keep the first baseline conservative. Daily close data does not need sub-daily seasonality.
    model = Prophet(
        daily_seasonality=False,
        weekly_seasonality=True,
        yearly_seasonality=False,
        interval_width=0.8,
    )
    model.fit(prophet_frame)

    # Use business days because market close forecasts should skip normal weekends.
    future = model.make_future_dataframe(periods=horizon_days, freq="B", include_history=False)
    forecast = model.predict(future)

    # Store a simple JSON-friendly array because this goes straight to Timescale JSONB later.
    forecast_values = [
        {
            "date": json_date(row["ds"]),
            "value": safe_float(row["yhat"]),
            "lower": safe_float(row["yhat_lower"]),
            "upper": safe_float(row["yhat_upper"]),
        }
        for _, row in forecast.iterrows()
    ]

    final_forecast = forecast_values[-1]
    final_price = float(final_forecast["value"])
    pct_change = (final_price - latest_close) / latest_close

    # Keep this return shape stable. The DB writer and ensemble layer depend on it.
    return {
        "ticker": ticker,
        "model": "prophet",
        "horizon_days": horizon_days,
        "direction": classify_direction(pct_change, neutral_threshold_pct),
        "confidence": confidence_from_interval(final_forecast, latest_close),
        "forecast_values": forecast_values,
    }


def prepare_prophet_frame(history: Any) -> pd.DataFrame:
    """Convert common OHLCV dataframe shapes into Prophet's ds/y format."""
    # The job may pass a normal dataframe, a yfinance frame, or something close to both.
    frame = pd.DataFrame(history).copy()
    frame = flatten_yfinance_columns(frame)

    close_column = find_column(frame, "Close", "close")
    if close_column is None:
        raise ValueError("history is missing Close")

    date_column = find_column(frame, "Date", "date", "Datetime", "datetime", "ds")
    if date_column is not None:
        ds = pd.to_datetime(frame[date_column])
    else:
        # If there is no explicit date column, use the dataframe index as market dates.
        ds = pd.to_datetime(frame.index)

    out = pd.DataFrame(
        {
            "ds": ds,
            "y": pd.to_numeric(frame[close_column], errors="coerce"),
        }
    )
    out = out.dropna(subset=["ds", "y"]).sort_values("ds")
    out = out.drop_duplicates(subset=["ds"], keep="last")

    if out.empty:
        raise ValueError("history has no usable close prices")
    if (out["y"] <= 0).any():
        raise ValueError("close prices must be positive")

    # Prophet expects timezone-naive timestamps.
    out["ds"] = out["ds"].dt.tz_localize(None)
    return out


def flatten_yfinance_columns(frame: pd.DataFrame) -> pd.DataFrame:
    # yfinance can return multi-index columns. Keep the price-field part.
    if getattr(frame.columns, "nlevels", 1) > 1:
        frame = frame.copy()
        frame.columns = [str(col[0]) for col in frame.columns]
    return frame


def find_column(frame: pd.DataFrame, *names: str) -> str | None:
    # Accept a few common names so the model is not tied to one dataframe source.
    wanted = {name.lower() for name in names}
    for column in frame.columns:
        if str(column).lower() in wanted:
            return str(column)
    return None


def classify_direction(expected_return: float, neutral_threshold_pct: float) -> str:
    # Small moves are "neutral" so the model does not over-signal noise.
    if expected_return > neutral_threshold_pct:
        return "up"
    if expected_return < -neutral_threshold_pct:
        return "down"
    return "neutral"


def confidence_from_interval(forecast: dict[str, Any], latest_close: float) -> float:
    """Convert forecast interval width into a simple 0..1 confidence score."""
    lower = float(forecast["lower"])
    upper = float(forecast["upper"])
    if latest_close <= 0 or upper <= lower:
        return 0.0

    # Narrower interval => higher confidence.
    interval_pct = (upper - lower) / latest_close
    confidence = 1.0 - min(interval_pct, 1.0)
    return round(max(0.0, min(confidence, 1.0)), 4)


def safe_float(value: Any) -> float | None:
    # Turn model values into plain Python floats and drop NaNs before JSON storage.
    if value is None:
        return None
    number = float(value)
    if math.isnan(number):
        return None
    return number


def json_date(value: Any) -> str:
    # Always return an ISO date string so forecast_values is easy to store and read back.
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)
