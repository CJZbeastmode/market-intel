"""Feature 5 — FRED API: macro series for sector recommendation"""
import os
import fredapi
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
FRED_API_KEY = os.getenv("FRED_API_KEY", "TODO_API_NEEDED")

SERIES = {
    "fed_funds_rate": "FEDFUNDS",
    "ten_year_yield": "GS10",
    "cpi_yoy":        "CPIAUCSL",
    "unemployment":   "UNRATE",
    "gdp_growth":     "A191RL1Q225SBEA",
}

if FRED_API_KEY == "TODO_API_NEEDED":
    raise SystemExit("Set FRED_API_KEY env var (free at https://fred.stlouisfed.org/docs/api/api_key.html)")

fred = fredapi.Fred(api_key=FRED_API_KEY)

print("=== FRED macro series (last 3 observations) ===")
for name, series_id in SERIES.items():
    data = fred.get_series(series_id)
    latest = data.dropna().tail(3)
    print(f"\n{name} ({series_id}):")
    print(latest.to_string())
