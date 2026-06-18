# Sprint 3 Interview Notes

## Goal

Sprint 3 adds the first analytics layer on top of the quote-ingestion pipeline.

The full flow is now:

1. Go Raft scheduler decides when `compute_indicators` should run.
2. Kafka executor publishes a job message into Redpanda.
3. Python `ml-worker` consumes that message.
4. Python job fetches recent price history from `yfinance`.
5. Native C++ code computes the indicators.
6. Python writes the latest indicator snapshot into TimescaleDB.

Sprint 2 was about moving work out of the scheduler. Sprint 3 is about adding native analytics behind that same pipeline.

## What Was Built

### 1. Native indicator module

Files:

- [ml/indicators/indicators.cpp](/Users/jay/Desktop/market-intel/ml/indicators/indicators.cpp)
- [ml/indicators/bindings.cpp](/Users/jay/Desktop/market-intel/ml/indicators/bindings.cpp)
- [ml/indicators/CMakeLists.txt](/Users/jay/Desktop/market-intel/ml/indicators/CMakeLists.txt)

This module exposes:

- `rsi`
- `sma`
- `ema`
- `macd`
- `atr`
- `bollinger`
- `obv`

Why this matters:

- technical indicators are pure math and fit well in native code
- pybind11 gives Python a clean interface
- this creates a path for faster analytics without redesigning the worker

### 2. Repeatable build path

File:

- [scripts/build_indicators.sh](/Users/jay/Desktop/market-intel/scripts/build_indicators.sh)

What it does:

- picks the right Python interpreter
- finds pybind11 for that interpreter
- runs CMake
- builds the extension
- smoke tests `import indicators`

Why this matters:

- build friction was the first Sprint 3 risk
- local build and Docker build now use the same basic path

### 3. Python indicator job

File:

- [ml/jobs/compute_indicators.py](/Users/jay/Desktop/market-intel/ml/jobs/compute_indicators.py)

What it does:

- reads tickers from payload or env
- downloads enough OHLCV history from `yfinance`
- flattens yfinance dataframe shape when needed
- converts data into plain float arrays
- calls the compiled `indicators` module
- writes the latest indicator values into TimescaleDB

Important idea:

- we compute full arrays, but persist only the newest snapshot values

### 4. Worker image update

File:

- [Dockerfile.ml](/Users/jay/Desktop/market-intel/Dockerfile.ml)

What changed:

- installs `build-essential` and `cmake`
- installs `pybind11`
- compiles the extension during image build

Why this matters:

- the worker container can run indicator jobs directly
- there is no manual native compile step after container start

### 5. Scheduler integration

File:

- [scripts/seed_jobs.sh](/Users/jay/Desktop/market-intel/scripts/seed_jobs.sh)

New seeded job:

```json
{
  "id": "compute_indicators",
  "name": "compute_indicators",
  "executor": "kafka",
  "payload": "jobs.ml:{\"job\":\"compute_indicators\"}"
}
```

This means the Go scheduler still only triggers work. It does not fetch market history and it does not compute indicators itself.

## Example Workflow

Concrete example with configured ticker `AAPL`:

1. Cron reaches the next `compute_indicators` time.
2. Current Raft leader sees the job is due.
3. Go Kafka executor publishes `{"job":"compute_indicators"}` to `jobs.ml`.
4. Python `ml-worker` consumes the message.
5. `ml.jobs.compute_indicators` downloads recent `AAPL` history.
6. C++ computes RSI, MACD, ATR, Bollinger Bands, OBV, EMA, and SMA.
7. Python writes the latest snapshot into the `indicators` table.

## What Changed Compared To A Normal Raft Scheduler

Normal Raft scheduler:

- stores jobs
- elects a leader
- runs small executors locally

This project after Sprint 3:

- still uses Raft for exactly-one scheduling decision
- still uses Kafka for async execution
- now uses a mixed Python + C++ worker path for analytics

Main differences:

1. The scheduler is a control plane, not the compute engine.

2. The worker is now multi-language.
   Python handles orchestration and I/O.
   C++ handles deterministic indicator math.

3. Quote ingestion and analytics are separate jobs.
   `fetch_quotes` and `compute_indicators` can run on different cadences.

4. The stored result is a snapshot row.
   We store the newest indicator values for the latest bar, not the whole arrays.

## Important Design Reasons

### Why C++ instead of pure Python

- indicator math is CPU work
- native code is a clean optimization target
- it is a contained step before heavier ML work later

### Why Python still owns the job

- `yfinance` access is already in Python
- the DB clients are already in Python
- pybind11 lets Python call native code without changing the worker model

### Why build the extension in the Docker image

- runtime startup stays simple
- deployment is closer to local verification
- container failures show up during image build instead of after boot

### Why `compute_indicators` is a separate scheduled job

- it can run less often than quote fetching
- failures stay isolated
- more derived jobs can be added later without changing the worker dispatcher

## Risks And Nuances To Mention

### Idempotency nuance

Indicator rows use the latest market bar timestamp, not the scheduler wall-clock run time.

That means:

- repeated runs on the same daily candle should not keep inserting duplicates
- a successful run may update nothing new if the source bar did not change

### History requirement

Sprint 3 indicators need enough bars to warm up.

For the current set:

- at least 50 rows are required

If not enough history exists, the job fails fast instead of writing partial junk.
