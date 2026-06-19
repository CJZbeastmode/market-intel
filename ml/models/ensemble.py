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
    # Sprint 4 has one real model only.
    # This wrapper exists so callers keep using one entrypoint even when more models arrive later.
    prediction = prophet_model.predict_close(
        ticker=ticker,
        history=history,
        horizon_days=horizon_days,
        neutral_threshold_pct=neutral_threshold_pct,
    )
    return as_ensemble_prediction(prediction)


def as_ensemble_prediction(prediction: dict[str, Any]) -> dict[str, Any]:
    """Normalize one model prediction into the public ensemble output shape."""
    # Keep the public output stable and hide which concrete model produced the first forecast.
    return {
        "ticker": prediction["ticker"],
        "model": ENSEMBLE_MODEL_NAME,
        "horizon_days": prediction["horizon_days"],
        "direction": prediction["direction"],
        "confidence": prediction["confidence"],
        "forecast_values": prediction["forecast_values"],
        # This makes future debugging easier once the ensemble has more than one component.
        "component_models": [prediction["model"]],
    }
