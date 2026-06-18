"""Feature 2 — Congressional trade disclosures via Quiver Quant.

Quiver Quant aggregates House + Senate STOCK Act PTR filings.
Endpoint is public (no key required as of testing).
Free API key available at quiverquant.com if auth is added later.
"""
import os
import httpx
from datetime import datetime, timedelta

PORTFOLIO_TICKERS = {"AAPL", "NVDA", "MSFT", "TSLA", "AMZN", "GOOGL"}
DAYS_BACK = 90
CUTOFF = (datetime.now() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")

QUIVER_API_KEY = os.getenv("QUIVER_API_KEY", "TODO_API_NEEDED")

headers = {"Accept": "application/json"}
if QUIVER_API_KEY != "TODO_API_NEEDED":
    headers["Authorization"] = f"Token {QUIVER_API_KEY}"

print("=== Quiver Quant: Congressional Trading ===")
resp = httpx.get(
    "https://api.quiverquant.com/beta/live/congresstrading",
    headers=headers,
    timeout=20,
)
resp.raise_for_status()
all_trades = resp.json()
print(f"Total records: {len(all_trades)}")

# Filter to recent portfolio tickers
hits = [
    t for t in all_trades
    if t.get("Ticker", "").upper() in PORTFOLIO_TICKERS
    and t.get("TransactionDate", "") >= CUTOFF
]
hits.sort(key=lambda x: x["TransactionDate"], reverse=True)

print(f"Portfolio hits in last {DAYS_BACK} days: {len(hits)}")
print()
for t in hits[:10]:
    print(
        f"  {t['TransactionDate']}  "
        f"{t['Ticker']:6}  "
        f"{t['Transaction']:10}  "
        f"{t['Range']:25}  "
        f"{t['Representative']}"
    )
