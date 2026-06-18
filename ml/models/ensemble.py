from typing import Any

from ml.models import prophet_model


ENSEMBLE_MODEL_NAME = "ensemble"


def predict(
    ticker: str,
    history: Any,
    horizon_days: int = prophet_model.DEFAULT_HORIZON_DAYS,
    neutral_threshold_pct: float = prophet_model.DEFAULT_NEUTRAL_THRESHOLD_PCT,
) -> dict[str, Any]:
    """Return the stable prediction contract through the ensemble interface."""
    # Sprint 4 has one real model. Keep the wrapper now so jobs do not change later.
    prediction = prophet_model.predict_close(
        ticker=ticker,
        history=history,
        horizon_days=horizon_days,
        neutral_threshold_pct=neutral_threshold_pct,
    )
    return as_ensemble_prediction(prediction)


def as_ensemble_prediction(prediction: dict[str, Any]) -> dict[str, Any]:
    """Normalize one model prediction into the public ensemble output shape."""
    return {
        "ticker": prediction["ticker"],
        "model": ENSEMBLE_MODEL_NAME,
        "horizon_days": prediction["horizon_days"],
        "direction": prediction["direction"],
        "confidence": prediction["confidence"],
        "forecast_values": prediction["forecast_values"],
        "component_models": [prediction["model"]],
    }
