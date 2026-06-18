"""Feature 7 — SEC EDGAR: 8-K filings via Atom feed (no API key needed)"""
import httpx
import xml.etree.ElementTree as ET

TICKERS = ["AAPL", "NVDA", "MSFT"]
HEADERS = {"User-Agent": "market-intel contact@example.com"}
ATOM_NS = "http://www.w3.org/2005/Atom"

def fetch_8k(ticker: str) -> list[dict]:
    url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={ticker}&type=8-K&dateb=&owner=include"
        f"&count=5&search_text=&output=atom"
    )
    resp = httpx.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    entries = root.findall(f"{{{ATOM_NS}}}entry")
    return [
        {
            "title":  e.findtext(f"{{{ATOM_NS}}}title", "").strip(),
            "date":   e.findtext(f"{{{ATOM_NS}}}updated", "")[:10],
            "url":    (e.find(f"{{{ATOM_NS}}}link") or {}).get("href", ""),
            "ticker": ticker,
        }
        for e in entries
    ]

print("=== SEC EDGAR 8-K filings ===")
for ticker in TICKERS:
    filings = fetch_8k(ticker)
    print(f"\n{ticker}: {len(filings)} recent 8-K filings")
    for f in filings:
        print(f"  {f['date']}  {f['title'][:60]}")
        print(f"           {f['url']}")
