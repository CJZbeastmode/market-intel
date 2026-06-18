"""Feature 1 — yfinance prices + Prophet 7-day forecast"""
import yfinance as yf
import pandas as pd
from prophet import Prophet

TICKER = "AAPL"

# --- yfinance ---
print("=== yfinance: last 2 days ===")
hist = yf.Ticker(TICKER).history(period="2d")
print(hist[["Open", "Close", "Volume"]])

# --- Prophet forecast ---
print("\n=== Prophet: 7-day forecast ===")
df = yf.Ticker(TICKER).history(period="90d")[["Close"]].reset_index()
df = df.rename(columns={"Date": "ds", "Close": "y"})
df["ds"] = df["ds"].dt.tz_localize(None)

m = Prophet(daily_seasonality=False, weekly_seasonality=True, yearly_seasonality=False)
m.fit(df)

future = m.make_future_dataframe(periods=7)
forecast = m.predict(future)
print(forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(7).to_string(index=False))
