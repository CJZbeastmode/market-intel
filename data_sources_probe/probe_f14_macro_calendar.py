"""Feature 14 — Macro Calendar: BLS data + FRED release history + BEA GDP.

BLS website schedule JSONs (bls.gov/schedule/...) block non-browser requests.
Working replacements:
  - BLS public API v2  : latest CPI and NFP readings (no key needed)
  - FRED release/dates : shows last release date per report (key needed)
  - BEA API            : GDP/PCE latest data (key needed)
"""
import os
import json
import urllib.request
import httpx
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
FRED_API_KEY = os.getenv("FRED_API_KEY", "TODO_API_NEEDED")
BEA_API_KEY  = os.getenv("BEA_API_KEY",  "TODO_API_NEEDED")
TODAY = datetime.utcnow().strftime("%Y-%m-%d")

# --- BLS API v2: latest CPI and NFP readings (no key) ---
print("=== BLS API v2: latest macro readings ===")
BLS_SERIES = {
    "CPI (all urban)":      "CUUR0000SA0",
    "Unemployment rate":    "LNS14000000",
    "Nonfarm payrolls MoM": "CES0000000001",
}
bls_payload = json.dumps({
    "seriesid": list(BLS_SERIES.values()),
    "startyear": "2026", "endyear": "2026",
}).encode()
req = urllib.request.Request(
    "https://api.bls.gov/publicAPI/v2/timeseries/data/",
    data=bls_payload,
    headers={"Content-Type": "application/json", "User-Agent": "market-intel/1.0"},
)
bls_data = json.loads(urllib.request.urlopen(req, timeout=15).read())
series_map = {s["seriesID"]: s["data"] for s in bls_data["Results"]["series"]}
for name, sid in BLS_SERIES.items():
    latest = series_map.get(sid, [{}])[0]
    print(f"  {name}: {latest.get('value','?')}  ({latest.get('periodName','?')} {latest.get('year','?')})")

# --- FRED: last release date + series value for key indicators ---
print("\n=== FRED: last release dates ===")
FRED_RELEASES = {"CPI": 10, "Employment Situation (NFP)": 50, "GDP": 53}
for name, rid in FRED_RELEASES.items():
    resp = httpx.get(
        "https://api.stlouisfed.org/fred/release/dates",
        params={"release_id": rid, "api_key": FRED_API_KEY,
                "sort_order": "desc", "limit": 1, "file_type": "json"},
        timeout=10,
    )
    resp.raise_for_status()
    dates = resp.json().get("release_dates", [])
    last = dates[0]["date"] if dates else "?"
    # Estimate next based on frequency
    if dates:
        last_dt = datetime.strptime(last, "%Y-%m-%d")
        freq_days = {"CPI": 30, "Employment Situation (NFP)": 30, "GDP": 90}
        next_est = (last_dt + timedelta(days=freq_days[name])).strftime("%Y-%m-%d")
    else:
        next_est = "?"
    print(f"  {name:30} last={last}  next≈{next_est}")

# --- BEA: latest GDP reading ---
print("\n=== BEA: latest GDP (NIPA) ===")
bea_resp = httpx.get(
    "https://apps.bea.gov/api/data",
    params={
        "UserID":       BEA_API_KEY,
        "method":       "GetData",
        "datasetname":  "NIPA",
        "TableName":    "T10101",
        "Frequency":    "Q",
        "Year":         "2026",
        "ResultFormat": "JSON",
    },
    timeout=15,
)
bea_resp.raise_for_status()
bea_data = bea_resp.json()
rows = bea_data.get("BEAAPI", {}).get("Results", {}).get("Data", [])
gdp_rows = [r for r in rows if r.get("LineDescription") == "Gross domestic product"]
for r in gdp_rows[-3:]:
    print(f"  {r.get('TimePeriod','?')}  GDP growth: {r.get('DataValue','?')}%")
