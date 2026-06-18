# DATA.md — Market Intel Platform
## Data Sources & Models by Feature

> Organized by feature. Every source listed is free.
> API keys marked with ★ require registration (all free).
> For demo purposes, synthetic fallbacks are provided for every feature
> so the dashboard runs without any API keys at all.

---

## Quick Setup for Demo

```bash
# Minimum keys to get real data flowing
# All optional — synthetic data fills gaps for demo

FRED_API_KEY=        # ★ free at https://fred.stlouisfed.org/docs/api/api_key.html
NEWS_API_KEY=        # ★ free at https://newsapi.org/register
REDDIT_CLIENT_ID=    # ★ free at https://www.reddit.com/prefs/apps
REDDIT_CLIENT_SECRET=

# No key needed: yfinance, SEC EDGAR, House/Senate Stock Watcher,
# CME FedWatch, BLS, FinBERT (runs locally), HuggingFace models
```

```bash
# Demo mode — runs with zero API keys, all synthetic data
DEMO_MODE=true docker compose up
```

---

## Feature 1 — Daily Analysis

**What it produces:** Comprehensive written brief per day covering portfolio
performance, ML forecasts, news context, and forward outlook.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| yfinance | Portfolio prices, day change, volume | `yf.Tickers(symbols).history(period='2d')` | ~2000/day |
| TimescaleDB | ML predictions (already computed) | SQL query on predictions table | Internal |
| Qdrant RAG | Recent news context per ticker | Semantic search, `days_back=2` | Internal |
| SEC EDGAR | Latest filing summaries | RSS feed, no key needed | 10 req/sec |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| DeepSeek (via OpenRouter) | Synthesize brief from all inputs | `deepseek/deepseek-chat` ~$0.001/call | Cloud |
| Prophet | 7-day price forecasts fed into brief | `pip install prophet` | Local |

### Demo fallback
```python
# If no API keys: use synthetic prices + canned news summaries
DEMO_PRICES = {'AAPL': 189.50, 'NVDA': 875.20, 'MSFT': 415.30}
DEMO_NEWS = "Markets steady ahead of Fed meeting. Tech sector outperforming."
```

---

## Feature 2 — Political Disclosures

**What it produces:** Flags congressional trades (House + Senate) that overlap
with your portfolio holdings, with trade size and representative name.

### Data sources

| Source | Data | Endpoint | Rate limit |
|---|---|---|---|
| House Stock Watcher | House STOCK Act filings | `https://housestockwatcher.com/api/transactions` | No limit |
| Senate Stock Watcher | Senate STOCK Act filings | `https://senatestockwatcher.com/api/transactions` | No limit |

### Implementation

```python
# ml/jobs/fetch_political_disclosures.py
import httpx, json
from datetime import datetime, timedelta

def fetch_congressional_trades(days_back: int = 30) -> list[dict]:
    trades = []
    for url in [
        'https://housestockwatcher.com/api/transactions',
        'https://senatestockwatcher.com/api/transactions'
    ]:
        resp = httpx.get(url, timeout=30)
        data = resp.json()
        # Filter to last N days and portfolio tickers only
        cutoff = datetime.now() - timedelta(days=days_back)
        for trade in data:
            trade_date = datetime.strptime(trade['transaction_date'], '%Y-%m-%d')
            if trade_date >= cutoff and trade['ticker'] in PORTFOLIO_TICKERS:
                trades.append({
                    'ticker':        trade['ticker'],
                    'representative': trade.get('representative', trade.get('senator')),
                    'transaction':   trade['type'],          # 'Purchase' or 'Sale'
                    'amount':        trade['amount'],        # range e.g. '$1,001-$15,000'
                    'date':          trade['transaction_date'],
                    'chamber':       'House' if 'housestockwatcher' in url else 'Senate'
                })
    return sorted(trades, key=lambda x: x['date'], reverse=True)
```

### Demo fallback
```python
DEMO_POLITICAL = [
    {'ticker': 'NVDA', 'representative': 'Nancy Pelosi',
     'transaction': 'Purchase', 'amount': '$1,000,001+', 'date': '2026-06-01'},
    {'ticker': 'MSFT', 'representative': 'Dan Sullivan',
     'transaction': 'Sale', 'amount': '$15,001-$50,000', 'date': '2026-05-28'},
]
```

---

## Feature 3 — Live Sentiment

**What it produces:** Per-ticker sentiment score (-1.0 to 1.0) updated intraday,
combining financial news and Reddit signals.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| NewsAPI | Financial headlines per ticker | `q={ticker}` search endpoint | 100/day ★ |
| PRAW (Reddit) | WSB + r/stocks + r/investing posts | `subreddit.search(ticker)` | 60/min ★ |
| Qdrant RAG | Already-embedded news (no API call) | Semantic search on existing docs | Internal |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| FinBERT | Financial sentiment classification | `ProsusAI/finbert` on HuggingFace | Local |
| VADER | Fast rule-based sentiment fallback | `pip install vaderSentiment` | Local |

```python
# ml/jobs/fetch_sentiment.py
from transformers import pipeline
import praw, os

# Load once at worker startup — not per-job
finbert = pipeline(
    'sentiment-analysis',
    model='ProsusAI/finbert',
    tokenizer='ProsusAI/finbert',
    device=-1  # CPU — fast enough for batch of 20 headlines
)

def score_headlines(headlines: list[str]) -> float:
    """Returns aggregate score: positive=+1, negative=-1, neutral=0"""
    if not headlines:
        return 0.0
    results = finbert(headlines[:10], truncation=True, max_length=512)
    scores = []
    for r in results:
        if r['label'] == 'positive':
            scores.append(r['score'])
        elif r['label'] == 'negative':
            scores.append(-r['score'])
        else:
            scores.append(0.0)
    return round(sum(scores) / len(scores), 4)

def fetch_reddit_mentions(ticker: str, limit: int = 25) -> list[str]:
    reddit = praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent='market-intel/1.0'
    )
    posts = []
    for sub in ['wallstreetbets', 'stocks', 'investing']:
        for post in reddit.subreddit(sub).search(ticker, limit=limit//3, time_filter='day'):
            posts.append(post.title)
    return posts
```

### Demo fallback
```python
# VADER scores from canned headlines — no API key needed
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()
DEMO_HEADLINES = {
    'AAPL': ['Apple reports record services revenue', 'iPhone sales beat expectations'],
    'NVDA': ['Nvidia data center demand surges', 'AI chip shortage easing'],
}
```

---

## Feature 4 — Stock Recommendation

**What it produces:** Buy / Hold / Sell signal with written reasoning per ticker,
combining ML forecast, sentiment, technicals, and fundamentals.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| TimescaleDB | ML predictions, indicators | SQL | Internal |
| TimescaleDB | Sentiment scores (from Feature 3) | SQL | Internal |
| yfinance | Fundamentals: P/E, EPS, revenue growth | `ticker.info` | ~2000/day |
| Qdrant RAG | News context for reasoning | Semantic search | Internal |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| Claude Sonnet (via OpenRouter) | Final Buy/Hold/Sell reasoning | `anthropic/claude-sonnet-4-6` ~$0.01/call | Cloud |
| Logistic regression | Indicator-based direction signal | `scikit-learn` | Local |

```python
# Fundamentals fetch — yfinance
def get_fundamentals(ticker: str) -> dict:
    info = yf.Ticker(ticker).info
    return {
        'pe_ratio':       info.get('trailingPE'),
        'eps':            info.get('trailingEps'),
        'revenue_growth': info.get('revenueGrowth'),
        'profit_margin':  info.get('profitMargins'),
        'debt_to_equity': info.get('debtToEquity'),
        'market_cap':     info.get('marketCap'),
    }
```

### Demo fallback
```python
DEMO_FUNDAMENTALS = {
    'AAPL': {'pe_ratio': 28.4, 'eps': 6.57, 'revenue_growth': 0.04},
    'NVDA': {'pe_ratio': 65.2, 'eps': 11.93, 'revenue_growth': 1.22},
}
DEMO_RECOMMENDATION = {'signal': 'Buy', 'confidence': 0.74,
                        'reasoning': 'Strong momentum + positive sentiment + beat estimates'}
```

---

## Feature 5 — Sector Recommendation

**What it produces:** Overweight / Neutral / Underweight per sector based on
ETF momentum, macro conditions, and rotation model.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| yfinance | Sector ETF prices (XLK, XLF, XLE, XLV, XLI, XLY, XLP, XLU, XLRE, XLB) | `yf.download(etf_list)` | ~2000/day |
| FRED API | Fed funds rate, 10Y yield, CPI, unemployment | `fredapi.Fred(api_key).get_series('GS10')` | 120/min ★ |
| yfinance | VIX (`^VIX`), DXY (`DX-Y.NYB`), 10Y yield (`^TNX`) | `yf.Ticker('^VIX').history()` | ~2000/day |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| Relative strength ranking | Rank sectors by 1M/3M/6M momentum | numpy, computed locally | Local |
| DeepSeek (via OpenRouter) | Macro interpretation + sector narrative | `deepseek/deepseek-chat` | Cloud |

```python
# Sector ETF tickers
SECTOR_ETFS = {
    'Technology':    'XLK',
    'Financials':    'XLF',
    'Energy':        'XLE',
    'Healthcare':    'XLV',
    'Industrials':   'XLI',
    'ConsumerDisc':  'XLY',
    'ConsumerStap':  'XLP',
    'Utilities':     'XLU',
    'RealEstate':    'XLRE',
    'Materials':     'XLB',
    'Communication': 'XLC',
}

# FRED macro series
FRED_SERIES = {
    'fed_funds_rate': 'FEDFUNDS',
    'ten_year_yield': 'GS10',
    'cpi_yoy':        'CPIAUCSL',
    'unemployment':   'UNRATE',
    'gdp_growth':     'A191RL1Q225SBEA',
}
```

### Demo fallback
```python
DEMO_SECTOR_SCORES = {
    'Technology': {'signal': 'Overweight',  'momentum_3m': 0.12},
    'Energy':     {'signal': 'Underweight', 'momentum_3m': -0.04},
    'Financials': {'signal': 'Neutral',     'momentum_3m': 0.02},
}
```

---

## Feature 6 — Entry / Exit Strategy

**What it produces:** Specific price levels for entry point, stop loss, and
price target per ticker based on technicals and ML forecast.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| TimescaleDB | OHLCV history for support/resistance | SQL — last 90 days | Internal |
| TimescaleDB | ATR (already computed by C++ job) | SQL on indicators table | Internal |
| TimescaleDB | ML 7-day forecast | SQL on predictions table | Internal |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| Pivot point calculation | Support / resistance levels | numpy, computed locally | Local |
| ATR-based stop loss | `entry - (2 × ATR)` | C++ extension output | Local |

```python
# Entry/exit calculation — pure math, no API needed
def compute_entry_exit(ticker: str, risk_profile: str = 'moderate') -> dict:
    db = TimescaleClient()

    # Get last 20 days OHLCV for pivot calculation
    rows = db.query("""
        SELECT high, low, close FROM ohlcv
        WHERE ticker = %s AND user_id = 'default'
        ORDER BY time DESC LIMIT 20
    """, (ticker,))

    highs  = [r['high']  for r in rows]
    lows   = [r['low']   for r in rows]
    closes = [r['close'] for r in rows]

    # Classic pivot point
    pivot      = (highs[0] + lows[0] + closes[0]) / 3
    resistance = (2 * pivot) - lows[0]
    support    = (2 * pivot) - highs[0]

    # ATR-based stop
    atr = db.query_one("""
        SELECT atr_14 FROM indicators
        WHERE ticker = %s AND user_id = 'default'
        ORDER BY time DESC LIMIT 1
    """, (ticker,))['atr_14']

    atr_multiplier = {'conservative': 1.5, 'moderate': 2.0, 'aggressive': 3.0}
    stop_distance  = atr * atr_multiplier.get(risk_profile, 2.0)

    # ML target
    pred = db.query_one("""
        SELECT forecast_values FROM predictions
        WHERE ticker = %s AND user_id = 'default'
        ORDER BY time DESC LIMIT 1
    """, (ticker,))

    target = pred['forecast_values'][-1]['value'] if pred else resistance * 1.05

    return {
        'entry':     round(support, 2),
        'stop_loss': round(support - stop_distance, 2),
        'target':    round(target, 2),
        'risk_reward': round((target - support) / stop_distance, 2),
        'pivot':     round(pivot, 2),
    }
```

### Demo fallback
```python
DEMO_ENTRY_EXIT = {
    'AAPL': {'entry': 185.20, 'stop_loss': 181.40, 'target': 198.50, 'risk_reward': 3.5}
}
```

---

## Feature 7 — Important News

**What it produces:** Ranked news relevant to your portfolio specifically,
not generic market news.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| NewsAPI | Fresh headlines for portfolio tickers | `q={ticker}` batch fetch | 100/day ★ |
| SEC EDGAR | Latest 8-K filings (material events) | RSS: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=8-K` | 10 req/sec |
| Qdrant RAG | Already-embedded news (zero API calls) | Semantic search per ticker | Internal |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| Llama 3.1 8B free (via OpenRouter) | Relevance scoring + one-line summary | `meta-llama/llama-3.1-8b-instruct:free` | Cloud |
| FinBERT | Sentiment tag per article | HuggingFace local | Local |

```python
# Fetch 8-K filings — real-time material event disclosures
import feedparser

def fetch_8k_filings(ticker: str) -> list[dict]:
    cik = get_cik_for_ticker(ticker)  # lookup from SEC EDGAR
    feed_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={cik}&type=8-K&dateb=&owner=include"
        f"&count=5&search_text=&output=atom"
    )
    feed = feedparser.parse(feed_url)
    return [
        {
            'title':   entry.title,
            'date':    entry.updated,
            'url':     entry.link,
            'ticker':  ticker,
            'type':    '8-K'
        }
        for entry in feed.entries
    ]
```

### Demo fallback
```python
DEMO_NEWS_FEED = [
    {'ticker': 'NVDA', 'headline': 'Nvidia announces $50B buyback program',
     'sentiment': 'positive', 'relevance': 0.94, 'source': 'Reuters'},
    {'ticker': 'AAPL', 'headline': 'Apple Vision Pro 2 release date leaked',
     'sentiment': 'positive', 'relevance': 0.81, 'source': 'Bloomberg'},
]
```

---

## Feature 8 — Live Indicators

**What it produces:** RSI, MACD, ATR streaming to dashboard via WebSocket,
updated every minute during market hours.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| yfinance | Live quote per ticker | `yf.Ticker(t).fast_info.last_price` | ~2000/day |
| Redis | Last 100 price ticks per ticker | `LRANGE quote:{user_id}:{ticker} 0 99` | Internal |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| C++ pybind11 extension | RSI, MACD, ATR, Bollinger, OBV, EMA, SMA | `ml/indicators/indicators.cpp` | Local |

```python
# Live indicator computation — runs every 1 minute via Kafka job
def compute_and_stream(ticker: str):
    # Pull last 100 ticks from Redis ring buffer
    raw = redis.lrange(f"quote:default:{ticker}", 0, 99)
    prices = [json.loads(p)['price'] for p in reversed(raw)]

    if len(prices) < 26:  # not enough data yet
        return

    result = {
        'ticker':  ticker,
        'rsi':     indicators.rsi(prices, 14)[-1],
        'macd':    indicators.macd(prices).macd[-1],
        'signal':  indicators.macd(prices).signal[-1],
        'atr':     indicators.atr(highs, lows, prices, 14)[-1],
        'time':    datetime.utcnow().isoformat(),
    }

    # Push to Redis pub/sub — WebSocket handler picks it up
    redis.publish(f"indicators:default:{ticker}", json.dumps(result))
```

### Demo fallback
```python
# Generate synthetic streaming indicators for demo
import math, time
def demo_indicator_stream(ticker: str):
    t = time.time()
    return {
        'rsi':    50 + 20 * math.sin(t / 30),   # oscillates 30-70
        'macd':   0.5 * math.sin(t / 60),
        'signal': 0.3 * math.sin(t / 60 - 0.5),
    }
```

---

## Feature 9 — Open Tab (Pre-Market Brief)

**What it produces:** Morning briefing 30 minutes before market open covering
overnight moves, futures, and macro events due today.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| yfinance | Futures: S&P (`ES=F`), Nasdaq (`NQ=F`), Dow (`YM=F`) | `yf.Ticker('ES=F').fast_info` | ~2000/day |
| yfinance | Overnight price changes (pre-market) | `yf.Ticker(t).history(prepost=True)` | ~2000/day |
| FRED API | Scheduled macro releases today | `fredapi` release calendar | 120/min ★ |
| CME FedWatch | Fed rate probability | `https://www.cmegroup.com/CmeWS/mvc/MarketData/getFedWatch` | No limit |
| Qdrant RAG | Overnight news (already embedded) | Semantic search, `days_back=1` | Internal |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| DeepSeek (via OpenRouter) | Synthesize morning brief | `deepseek/deepseek-chat` | Cloud |

```python
# CME FedWatch — free, no key
def get_fed_probabilities() -> dict:
    resp = httpx.get(
        'https://www.cmegroup.com/CmeWS/mvc/MarketData/getFedWatch',
        headers={'User-Agent': 'market-intel/1.0'},
        timeout=10
    )
    data = resp.json()
    # Returns probabilities of rate hike/hold/cut at next meeting
    return {
        'hike_prob': data.get('hikeProb', 0),
        'hold_prob': data.get('holdProb', 0),
        'cut_prob':  data.get('cutProb', 0),
        'next_meeting': data.get('nextMeetingDate', '')
    }

# Scheduled releases today — FRED
def get_todays_releases() -> list[str]:
    import fredapi
    fred = fredapi.Fred(api_key=os.getenv('FRED_API_KEY'))
    releases = fred.search_by_release(release_id=None)
    today = datetime.utcnow().date()
    return [r for r in releases if r.get('release_date') == str(today)]
```

### Demo fallback
```python
DEMO_PREMARKET = {
    'futures': {'ES=F': +0.42, 'NQ=F': +0.61, 'YM=F': +0.38},
    'fed_hold_prob': 0.82,
    'events_today': ['CPI 8:30am ET', 'Fed Chair speaks 2:00pm ET'],
    'brief': 'Futures pointing higher ahead of CPI print. Markets pricing 82% hold.'
}
```

---

## Feature 10 — Close Tab (Post-Market Summary)

**What it produces:** Evening summary 30 minutes after market close with day's
P&L, notable moves, and portfolio delta.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| yfinance | End-of-day OHLCV | `yf.download(tickers, period='1d')` | ~2000/day |
| TimescaleDB | Previous close for delta calculation | SQL on ohlcv table | Internal |
| TimescaleDB | Portfolio holdings | SQL on portfolio table | Internal |
| Qdrant RAG | Today's news context | Semantic search, `days_back=1` | Internal |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| DeepSeek (via OpenRouter) | Narrative summary of day | `deepseek/deepseek-chat` | Cloud |

### Demo fallback
```python
DEMO_CLOSE = {
    'portfolio_pnl_today': +1842.30,
    'portfolio_pnl_pct':   +0.94,
    'best_performer':  {'ticker': 'NVDA', 'change_pct': +3.21},
    'worst_performer': {'ticker': 'TSLA', 'change_pct': -1.87},
    'brief': 'Strong session led by AI names. NVDA +3.2% on data center upgrade cycle news.'
}
```

---

## Feature 11 — Earnings Surprise Prediction

**What it produces:** Beat / miss probability before earnings, combining analyst
revisions, options implied move, and insider activity.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| yfinance | Analyst EPS estimates + revisions | `ticker.earnings_dates`, `ticker.recommendations` | ~2000/day |
| yfinance | Options chain for implied move | `ticker.option_chain(nearest_expiry)` | ~2000/day |
| SEC EDGAR | Form 4 insider activity (last 90 days) | RSS feed per CIK | 10 req/sec |
| TimescaleDB | Historical earnings surprise record | SQL on earnings_calendar table | Internal |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| Logistic regression | Beat probability from historical patterns | `scikit-learn` | Local |
| ATM straddle pricing | Options-implied expected move | numpy calculation | Local |

```python
# Options-implied expected move — professional-grade, free via yfinance
def get_implied_move(ticker: str) -> float:
    """
    Implied move = (ATM call price + ATM put price) / stock price
    This is what options market makers price as the expected earnings swing.
    """
    t = yf.Ticker(ticker)
    price = t.fast_info.last_price

    # Get nearest expiry after earnings date
    expiries = t.options
    earnings_date = t.earnings_dates.index[0].date()
    nearest_expiry = min(
        (e for e in expiries if datetime.strptime(e, '%Y-%m-%d').date() >= earnings_date),
        default=expiries[0]
    )

    chain = t.option_chain(nearest_expiry)
    atm_strike = min(chain.calls['strike'], key=lambda x: abs(x - price))

    call_price = chain.calls[chain.calls['strike'] == atm_strike]['lastPrice'].values[0]
    put_price  = chain.puts[chain.puts['strike']  == atm_strike]['lastPrice'].values[0]

    return round((call_price + put_price) / price, 4)  # e.g. 0.085 = ±8.5% expected move

# SEC Form 4 insider trades — free, real-time
def get_insider_activity(ticker: str, days: int = 90) -> list[dict]:
    cik = get_cik_for_ticker(ticker)
    feed = feedparser.parse(
        f"https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcompany&CIK={cik}&type=4&dateb=&owner=include&count=10&output=atom"
    )
    return [
        {
            'filer':       e.title,
            'date':        e.updated,
            'filing_url':  e.link,
        }
        for e in feed.entries
    ]
```

### Demo fallback
```python
DEMO_EARNINGS_PRED = {
    'NVDA': {
        'beat_probability': 0.74,
        'implied_move':     0.092,  # ±9.2%
        'analyst_revision': 'up',
        'insider_signal':   'neutral',
        'recommendation':   'Beat likely — strong analyst revision trend'
    }
}
```

---

## Feature 12 — Correlation Tracker

**What it produces:** Holdings correlation matrix plus VIX and yield curve
exposure per position.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| TimescaleDB | 90-day price history per ticker | SQL on ohlcv table | Internal |
| yfinance | VIX (`^VIX`), 10Y yield (`^TNX`), DXY (`DX-Y.NYB`) | `yf.download(['^VIX','^TNX'])` | ~2000/day |
| FRED API | Yield curve (2Y vs 10Y spread) | `fred.get_series('T10Y2Y')` | 120/min ★ |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| Pearson correlation matrix | Holdings pairwise correlation | `pandas.DataFrame.corr()` | Local |
| Beta calculation | Sensitivity to SPY | numpy regression | Local |

```python
# Full correlation analysis — pure pandas/numpy, no external calls
def compute_correlation_matrix(tickers: list[str], days: int = 90) -> pd.DataFrame:
    db = TimescaleClient()
    price_data = {}
    for ticker in tickers:
        rows = db.query("""
            SELECT time::date AS date, close
            FROM ohlcv WHERE ticker = %s AND user_id = 'default'
            AND time >= NOW() - INTERVAL '%s days'
            ORDER BY time ASC
        """, (ticker, days))
        price_data[ticker] = {r['date']: r['close'] for r in rows}

    df = pd.DataFrame(price_data)
    returns = df.pct_change().dropna()
    return returns.corr().round(3)

# VIX + yield exposure
def get_macro_exposure() -> dict:
    macro = yf.download(['^VIX', '^TNX', 'DX-Y.NYB', 'SPY'],
                         period='90d', interval='1d', auto_adjust=True)
    return {
        'vix_current':   float(macro['Close']['^VIX'].iloc[-1]),
        'yield_10y':     float(macro['Close']['^TNX'].iloc[-1]),
        'dxy':           float(macro['Close']['DX-Y.NYB'].iloc[-1]),
    }
```

### Demo fallback
```python
import numpy as np
DEMO_CORRELATION = pd.DataFrame(
    np.array([[1.0, 0.72, 0.68, 0.31],
              [0.72, 1.0, 0.61, 0.28],
              [0.68, 0.61, 1.0, 0.22],
              [0.31, 0.28, 0.22, 1.0]]),
    index=['AAPL','NVDA','MSFT','TSLA'],
    columns=['AAPL','NVDA','MSFT','TSLA']
)
```

---

## Feature 13 — Options Flow Monitor

**What it produces:** Unusual options activity flagged for your tickers using
volume-to-open-interest ratio analysis.

> Note: Real sweep order data (Unusual Whales, Tradier) requires paid APIs.
> This feature uses yfinance options chain as a free proxy — flags tickers
> where options volume exceeds 3× the 20-day average. Weaker signal than
> real flow data but honest and free.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| yfinance | Options chain per ticker (all expiries) | `ticker.option_chain(expiry)` | ~2000/day |
| TimescaleDB | Historical average options volume | SQL — computed and stored daily | Internal |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| Volume/OI ratio | Unusual activity flag | numpy, computed locally | Local |
| Put/call ratio | Sentiment from options market | numpy, computed locally | Local |

```python
# Options unusual activity — free proxy via yfinance
def detect_unusual_options(ticker: str) -> list[dict]:
    t = yf.Ticker(ticker)
    flags = []

    for expiry in t.options[:4]:  # check nearest 4 expiries only
        chain = t.option_chain(expiry)

        for side, df in [('call', chain.calls), ('put', chain.puts)]:
            # Flag strikes where volume > 3x open interest
            unusual = df[
                (df['volume'] > 0) &
                (df['openInterest'] > 0) &
                (df['volume'] / df['openInterest'] > 3.0)
            ]
            for _, row in unusual.iterrows():
                flags.append({
                    'ticker':        ticker,
                    'expiry':        expiry,
                    'strike':        row['strike'],
                    'type':          side,
                    'volume':        int(row['volume']),
                    'open_interest': int(row['openInterest']),
                    'vol_oi_ratio':  round(row['volume'] / row['openInterest'], 2),
                    'impl_vol':      round(row['impliedVolatility'], 4),
                })

    return sorted(flags, key=lambda x: x['vol_oi_ratio'], reverse=True)[:10]
```

### Demo fallback
```python
DEMO_OPTIONS_FLOW = [
    {'ticker': 'NVDA', 'type': 'call', 'strike': 900, 'expiry': '2026-07-18',
     'volume': 15420, 'open_interest': 2100, 'vol_oi_ratio': 7.3, 'impl_vol': 0.52},
]
```

---

## Feature 14 — Macro Calendar

**What it produces:** This week's market-moving scheduled events with
Fed probability context and estimated impact per event.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| FRED API | CPI, NFP, GDP release schedule | `fredapi` release calendar endpoint | 120/min ★ |
| BLS | Jobs report + CPI exact dates | `https://www.bls.gov/schedule/news_release/` | No limit |
| BEA | GDP + PCE release dates | `https://apps.bea.gov/api/data?UserID=...&method=GetReleaseDates` | No limit ★ |
| CME FedWatch | Fed meeting probability | Free JSON endpoint (see Feature 9) | No limit |
| yfinance | VIX term structure (volatility around events) | `yf.download('^VIX')` | ~2000/day |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| Llama 3.1 8B free (via OpenRouter) | Impact assessment per event | `meta-llama/llama-3.1-8b-instruct:free` | Cloud |

```python
# BLS release calendar — free, no key
def get_bls_calendar() -> list[dict]:
    resp = httpx.get(
        'https://www.bls.gov/schedule/news_release/cpi.json',
        headers={'User-Agent': 'market-intel/1.0'}
    )
    # Returns scheduled CPI release dates for the year
    data = resp.json()
    return [
        {
            'event':       'CPI',
            'date':        item['date'],
            'time':        '08:30 ET',
            'impact':      'high',
        }
        for item in data.get('releases', [])
        if item['date'] >= datetime.utcnow().strftime('%Y-%m-%d')
    ][:4]  # next 4 releases only
```

### Demo fallback
```python
DEMO_MACRO_CALENDAR = [
    {'event': 'CPI YoY', 'date': '2026-06-25', 'time': '08:30 ET',
     'impact': 'high', 'previous': '3.2%', 'estimate': '3.1%'},
    {'event': 'FOMC Meeting', 'date': '2026-07-01', 'time': '14:00 ET',
     'impact': 'high', 'fed_hold_prob': 0.82},
    {'event': 'NFP', 'date': '2026-07-05', 'time': '08:30 ET',
     'impact': 'high', 'previous': '+175k', 'estimate': '+185k'},
]
```

---

## Feature 15 — Position Sizing

**What it produces:** Suggested position size per ticker using Kelly criterion
and fixed fractional methods based on ATR and risk profile config.

### Data sources

| Source | Data | How | Rate limit |
|---|---|---|---|
| TimescaleDB | ATR per ticker (already computed) | SQL on indicators table | Internal |
| TimescaleDB | ML win rate per model | SQL on predictions + outcomes | Internal |
| TimescaleDB | Portfolio current holdings | SQL on portfolio table | Internal |

### Models

| Model | Purpose | Source | Runs |
|---|---|---|---|
| Kelly criterion | Optimal position size from win rate + payoff | numpy formula | Local |
| Fixed fractional | 1-2% portfolio risk per trade | numpy formula | Local |
| ATR position sizing | Risk-normalised size across tickers | C++ ATR output | Local |

```python
# Position sizing — pure math, zero external calls
def compute_position_size(
    ticker: str,
    portfolio_value: float,
    risk_profile: str = 'moderate'
) -> dict:
    db = TimescaleClient()

    atr = db.query_one("""
        SELECT atr_14 FROM indicators
        WHERE ticker = %s AND user_id = 'default'
        ORDER BY time DESC LIMIT 1
    """, (ticker,))['atr_14']

    price = db.query_one("""
        SELECT price FROM market_quotes
        WHERE ticker = %s AND user_id = 'default'
        ORDER BY time DESC LIMIT 1
    """, (ticker,))['price']

    # Risk per trade based on profile
    risk_pct = {'conservative': 0.01, 'moderate': 0.02, 'aggressive': 0.03}
    risk_amount = portfolio_value * risk_pct.get(risk_profile, 0.02)

    # ATR-based: how many shares so that 2×ATR move = risk_amount
    stop_distance = 2 * atr
    shares_atr = risk_amount / stop_distance

    # Fixed fractional: % of portfolio in this position
    fixed_pct = {'conservative': 0.05, 'moderate': 0.10, 'aggressive': 0.15}
    shares_fixed = (portfolio_value * fixed_pct.get(risk_profile, 0.10)) / price

    # Kelly — requires historical win rate from predictions vs actuals
    kelly_fraction = compute_kelly(ticker, db)
    shares_kelly = (portfolio_value * kelly_fraction) / price if kelly_fraction else None

    return {
        'ticker':       ticker,
        'price':        round(price, 2),
        'atr_sizing':   {'shares': int(shares_atr),
                         'value':  round(shares_atr * price, 2),
                         'stop':   round(price - stop_distance, 2)},
        'fixed_sizing': {'shares': int(shares_fixed),
                         'value':  round(shares_fixed * price, 2)},
        'kelly_sizing': {'shares': int(shares_kelly),
                         'fraction': round(kelly_fraction, 4)} if shares_kelly else None,
        'risk_amount':  round(risk_amount, 2),
    }

def compute_kelly(ticker: str, db) -> float:
    """Kelly fraction from historical prediction accuracy."""
    rows = db.query("""
        SELECT p.direction,
               CASE WHEN o.close > o_prev.close THEN 'up' ELSE 'down' END AS actual
        FROM predictions p
        JOIN ohlcv o      ON p.ticker = o.ticker
            AND DATE(o.time) = DATE(p.time) + INTERVAL '7 days'
        JOIN ohlcv o_prev ON p.ticker = o_prev.ticker
            AND DATE(o_prev.time) = DATE(p.time)
        WHERE p.ticker = %s AND p.user_id = 'default'
        ORDER BY p.time DESC LIMIT 100
    """, (ticker,))

    if len(rows) < 20:
        return 0.0  # not enough history

    wins  = sum(1 for r in rows if r['direction'] == r['actual'])
    total = len(rows)
    win_rate  = wins / total
    loss_rate = 1 - win_rate
    avg_payoff = 1.5  # assume 1.5:1 reward:risk — replace with actual from entry/exit

    kelly = win_rate - (loss_rate / avg_payoff)
    return max(0.0, min(kelly * 0.5, 0.15))  # half-Kelly, capped at 15%
```

### Demo fallback
```python
DEMO_POSITION_SIZE = {
    'AAPL': {
        'atr_sizing':   {'shares': 52, 'value': 9854.00, 'stop': 181.40},
        'fixed_sizing': {'shares': 106, 'value': 20087.00},
        'kelly_sizing': {'shares': 38, 'fraction': 0.072},
        'risk_amount':  400.00
    }
}
```

---

## Global Demo Mode

Add this to `ml/demo.py`. When `DEMO_MODE=true`, every feature returns
synthetic data instantly with no API calls, no database, no keys needed.

```python
# ml/demo.py
import os

DEMO_MODE = os.getenv('DEMO_MODE', 'false').lower() == 'true'

def demo_or_real(demo_data, real_fn, *args, **kwargs):
    """
    Use demo data if DEMO_MODE=true, otherwise call the real function.
    Wrap every feature function with this for instant demo capability.

    Usage:
        result = demo_or_real(DEMO_PRICES, fetch_live_prices, tickers)
    """
    if DEMO_MODE:
        return demo_data
    return real_fn(*args, **kwargs)
```

```bash
# Run full demo with zero setup — all 15 features active
DEMO_MODE=true TICKERS=AAPL,NVDA,MSFT,TSLA docker compose up

# Gradually switch to real data as you get API keys
DEMO_MODE=false NEWS_API_KEY=xxx FRED_API_KEY=xxx docker compose up
```

---

## API Key Summary

| Key | Feature(s) | Where to get | Cost |
|---|---|---|---|
| `FRED_API_KEY` | Sector rec, Open tab, Correlation, Macro calendar | https://fred.stlouisfed.org/docs/api/api_key.html | Free |
| `NEWS_API_KEY` | Daily analysis, Live sentiment, Important news | https://newsapi.org/register | Free (100/day) |
| `REDDIT_CLIENT_ID` + `SECRET` | Live sentiment | https://www.reddit.com/prefs/apps | Free |
| `OPENROUTER_API_KEY` | All LLM features | https://openrouter.ai | Free tier + pay per use |
| `BEA_API_KEY` | Macro calendar | https://apps.bea.gov/API/signup/ | Free |
| None needed | Political disclosures, Entry/exit, Options flow, Position sizing, Live indicators, Close tab, Earnings pred (options), Correlation (local math) | — | Free |

---

## NewsAPI Budget (100 calls/day)

All features that need news must query Qdrant RAG, not NewsAPI directly.
Only `fetch_news` job calls NewsAPI. All other features read from Qdrant.

| Job | Calls/day | Notes |
|---|---|---|
| `fetch_news` (every 30 min) | 48 | Primary ingestion |
| Buffer for development | 52 | Do not use in production jobs |
| **Total** | **100/100** | Exactly at limit — do not add more NewsAPI callers |

---

*Read alongside FORWARD.md (architecture), SCALING.md (scalability), SPRINTS.md (timeline).*
*Demo mode lets you present all 15 features before a single API key is obtained.*