"""Feature 9 — Fed rate probabilities.

CME FedWatch blocks all automated access (IP ban + scraping policy).
This probe replicates the same calculation using:
  - yfinance ZQ=F  : front-month Fed funds futures (implied rate = 100 - price)
  - NY Fed API     : current EFFR and official target range (no key needed)

Probability method (same as FedWatch):
  P(cut)  = (midpoint - implied) / 0.25
  P(hike) = (implied - midpoint) / 0.25
  P(hold) = 1 - P(cut) - P(hike)   [clamped to 0-1]
"""
import json
import urllib.request
import yfinance as yf

# --- NY Fed: current target range ---
print("=== NY Fed: current EFFR + target range ===")
req = urllib.request.Request(
    "https://markets.newyorkfed.org/api/rates/all/latest.json",
    headers={"User-Agent": "market-intel/1.0"},
)
data = json.loads(urllib.request.urlopen(req, timeout=10).read())
effr = next(r for r in data["refRates"] if r["type"] == "EFFR")
target_low  = effr["targetRateFrom"]
target_high = effr["targetRateTo"]
target_mid  = (target_low + target_high) / 2
print(f"  EFFR:          {effr['percentRate']}%")
print(f"  Target range:  {target_low}% – {target_high}%  (mid: {target_mid}%)")
print(f"  Date:          {effr['effectiveDate']}")

# --- yfinance: Fed funds futures implied rate ---
print("\n=== Fed funds futures (ZQ=F) ===")
zq = yf.Ticker("ZQ=F")
hist = zq.history(period="5d")
futures_price   = hist["Close"].iloc[-1]
implied_rate    = round(100 - futures_price, 4)
print(f"  ZQ=F price:    {futures_price:.4f}")
print(f"  Implied rate:  {implied_rate}%")

# --- Derive probabilities ---
print("\n=== Implied Fed probabilities ===")
diff = implied_rate - target_mid
p_hike = max(0.0, round( diff / 0.25, 4))
p_cut  = max(0.0, round(-diff / 0.25, 4))
p_hold = round(max(0.0, 1.0 - p_hike - p_cut), 4)

print(f"  P(hike 25bp):  {p_hike:.1%}")
print(f"  P(hold):       {p_hold:.1%}")
print(f"  P(cut 25bp):   {p_cut:.1%}")

# --- Bonus: SOFR term structure (forward rate path) ---
print("\n=== SOFR futures term structure ===")
for sym, label in [("SR1=F", "1-month SOFR"), ("SR3=F", "3-month SOFR")]:
    h = yf.Ticker(sym).history(period="2d")
    if not h.empty:
        p = h["Close"].iloc[-1]
        print(f"  {label} ({sym}): {p:.4f}  → implied {100 - p:.4f}%")
