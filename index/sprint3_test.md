# Sprint 3 Test Guide

## Purpose

This file lists the simple test scenarios for Sprint 3.

Main Sprint 3 goal:

- prove that the native indicator module builds and imports
- prove that the scheduler can trigger indicator computation end to end

## Prerequisites

- Docker Desktop running
- Compose services available
- network access for `yfinance`
- local Python environment with `pybind11` for local native build testing

## Scenario 1: Build the native module locally

Command:

```bash
./scripts/build_indicators.sh
```

Expected:

- CMake configure succeeds
- CMake build succeeds
- script finishes by importing `indicators`

Why this matters:

- proves the native toolchain works before Kafka or Docker is involved

## Scenario 2: Import test

Command:

```bash
.venv/bin/python -c "import indicators; print(indicators.rsi([1,2,3,2,4,5,6,7,8,9,10,11,12,11,13], 14)[-1])"
```

Expected:

- import succeeds
- a numeric RSI value is printed

## Scenario 3: Indicator smoke test

Command:

```bash
.venv/bin/python -c "import indicators; prices=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60]; highs=[p+1 for p in prices]; lows=[p-1 for p in prices]; volumes=[1000]*len(prices); print(indicators.sma(prices,20)[-1], indicators.ema(prices,20)[-1], indicators.macd(prices).histogram[-1], indicators.atr(highs,lows,prices,14)[-1], indicators.bollinger(prices,20,2.0).upper[-1], indicators.obv(prices,volumes)[-1])"
```

Expected:

- all functions return numbers
- no binding or import error appears

## Scenario 4: Compute job syntax test

Command:

```bash
.venv/bin/python -m py_compile ml/jobs/compute_indicators.py
```

Expected:

- no syntax errors

## Scenario 5: Direct compute job run

Command:

```bash
TICKERS=NVDA DB_HOST=localhost DB_PORT=5432 DB_NAME=marketintel DB_USER=marketintel DB_PASSWORD=marketintel_dev REDIS_URL=redis://localhost:6379/0 .venv/bin/python -m ml.jobs.compute_indicators
```

Expected:

- job logs start
- job logs stored indicators for `NVDA`
- job logs finish successfully

Why this matters:

- isolates indicator logic from Kafka scheduling

## Scenario 6: Worker image build

Command:

```bash
docker compose build ml-worker
```

Expected:

- image build succeeds
- native extension compiles during image build

## Scenario 7: Worker container import test

Command:

```bash
docker compose exec -T ml-worker python -c "import indicators; print(indicators.rsi([1,2,3,2,4,5,6,7,8,9,10,11,12,11,13], 14)[-1])"
```

Expected:

- import succeeds inside the container

## Scenario 8: Seed scheduler jobs

Command:

```bash
./scripts/seed_jobs.sh
```

Expected:

- script finds the current leader API
- recreates `fetch_live_quotes`
- recreates `compute_indicators`

Check:

```bash
curl http://127.0.0.1:8081/jobs/compute_indicators
```

Expected:

- executor is `kafka`
- payload is `jobs.ml:{"job":"compute_indicators"}`

## Scenario 9: End-to-end scheduled indicator run

Steps:

1. Start Compose.
2. Run `./scripts/init_topics.sh`.
3. Run `./scripts/seed_jobs.sh`.
4. Wait for the next `compute_indicators` cron tick.

Check scheduler:

```bash
curl http://127.0.0.1:8081/jobs/compute_indicators
```

Expected:

- `last_status` is `success`
- `last_run` is set

Check worker logs:

```bash
docker compose logs --tail=200 ml-worker
```

Expected:

- `dispatching job=compute_indicators`
- `stored indicators ticker=AAPL` or another configured ticker

Check TimescaleDB:

```bash
docker compose exec -T timescaledb psql -U marketintel -d marketintel -c "SELECT time, ticker, rsi_14, macd, macd_signal, macd_hist, bb_upper, bb_middle, bb_lower, atr_14, obv, ema_20, sma_50 FROM indicators ORDER BY time DESC, ticker ASC LIMIT 10;"
```

Expected:

- at least one row exists for the configured ticker

## Scenario 10: Idempotency behavior

Command:

```bash
docker compose exec -T timescaledb psql -U marketintel -d marketintel -c "SELECT time, ticker, COUNT(*) FROM indicators GROUP BY time, ticker ORDER BY time DESC;"
```

Expected:

- repeated runs on the same daily bar do not keep creating duplicate rows for the same logical snapshot

Why this matters:

- confirms safe reruns and safe retries
