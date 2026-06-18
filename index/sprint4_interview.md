# Sprint 4 Interview Notes

## Goal

Sprint 4 adds the first forecasting layer to the pipeline.

The full flow is now:

1. Go Raft scheduler decides when `run_predictions` should run.
2. Kafka executor publishes a job message into Redpanda.
3. Python `ml-worker` consumes that message.
4. Python job fetches recent daily price history from `yfinance`.
5. Prophet builds a 7-day forecast.
6. Python writes one prediction row per ticker into TimescaleDB.

Sprint 3 gave us technical indicators. Sprint 4 adds forward-looking predictions on top of the same scheduler and worker pipeline.

## What Was Built

### 1. Prophet baseline model

File:

- [ml/models/prophet_model.py](/Users/jay/Desktop/market-intel/ml/models/prophet_model.py)

What it does:

- takes one ticker plus daily close history
- converts data into Prophet `ds` / `y` format
- trains a small Prophet model
- forecasts 7 business days ahead
- returns a stable prediction contract

Returned fields:

- `ticker`
- `model`
- `horizon_days`
- `direction`
- `confidence`
- `forecast_values`

Why this matters:

- it gives us a working baseline before deeper models
- it locks the output shape early so later jobs and APIs do not churn

### 2. Thin ensemble wrapper

File:

- [ml/models/ensemble.py](/Users/jay/Desktop/market-intel/ml/models/ensemble.py)

What it does:

- wraps the Prophet baseline
- returns the same public contract
- labels the output as `model="ensemble"`

Why this matters:

- Sprint 4 still runs one real model
- later we can add `N-BEATS` behind this interface without changing the job contract

### 3. Prediction DB write support

File:

- [ml/db/timescale.py](/Users/jay/Desktop/market-intel/ml/db/timescale.py)

What changed:

- added `upsert_prediction()`
- added `prediction_idempotency_key()`
- stores `forecast_values` as JSONB

Important columns written into `predictions`:

- `time`
- `user_id`
- `ticker`
- `model`
- `horizon_days`
- `direction`
- `confidence`
- `forecast_values`
- `idempotency_key`

Why this matters:

- prediction rows now use the same idempotent-write pattern as earlier sprints

### 4. Scheduled prediction job

File:

- [ml/jobs/run_predictions.py](/Users/jay/Desktop/market-intel/ml/jobs/run_predictions.py)

What it does:

- reads tickers from payload or env
- fetches about 90 days of daily history
- calls `ml.models.ensemble.predict()`
- writes one prediction row per ticker
- logs partial failures without dropping successful tickers

Important idea:

- each ticker becomes one prediction snapshot row
- forecast arrays stay inside `forecast_values` JSONB

### 5. Worker image update

Files:

- [requirements.ml-worker.txt](/Users/jay/Desktop/market-intel/requirements.ml-worker.txt)
- [Dockerfile.ml](/Users/jay/Desktop/market-intel/Dockerfile.ml)

What changed:

- added `prophet` to the worker dependency set
- rebuilt `ml-worker`
- verified the container can import Prophet and run `run_predictions`

Why this matters:

- Sprint 4 is not complete until the container path works, not just the local venv

### 6. Scheduler integration

File:

- [scripts/seed_jobs.sh](/Users/jay/Desktop/market-intel/scripts/seed_jobs.sh)

New seeded job:

```json
{
  "id": "run_predictions",
  "name": "run_predictions",
  "executor": "kafka",
  "payload": "jobs.ml:{\"job\":\"run_predictions\"}"
}
```

Default schedule:

- `30 21 * * 1-5`

That is the weekday after-close schedule currently used in UTC.

## Example Workflow

Concrete example with `AAPL`:

1. Cron reaches the next `run_predictions` time.
2. Current Raft leader sees the job is due.
3. Go Kafka executor publishes `{"job":"run_predictions"}` to `jobs.ml`.
4. Python `ml-worker` consumes the message.
5. `ml.jobs.run_predictions` downloads about 90 days of daily `AAPL` history.
6. `ml.models.ensemble` calls the Prophet baseline.
7. Prophet returns direction, confidence, and 7 forecast points.
8. Python stores one `predictions` row in TimescaleDB.

## What Changed Compared To A Normal Raft Scheduler

Normal Raft scheduler:

- stores jobs
- elects a leader
- runs small executors locally

This project after Sprint 4:

- still uses Raft for exactly-one scheduling decision
- still uses Kafka for async execution
- now includes a forecasting stage in the Python worker

Main differences:

1. The scheduler still does not do the ML work itself.

2. Forecasting is just another scheduled data pipeline job.
   That keeps prediction cadence separate from quote and indicator cadence.

3. We now persist forward-looking output, not only historical snapshots.

4. The model interface is already abstracted.
   Prophet is the current implementation.
   Ensemble is the public contract.

## Important Design Reasons

### Why Prophet first

- easy baseline
- lighter than deep learning
- easier to debug when building the first prediction pipeline

### Why 90 days of daily history

- enough recent rows for a usable baseline
- still small enough to keep the first implementation simple and quick

### Why direction is not a separate hardcoded rule set

- it is derived from latest close versus final forecast close
- keeps the signal tied to the model output

### Why confidence is interval-based

- confidence comes from forecast width
- narrower intervals mean more confidence
- avoids fake constant confidence numbers

### Why `ensemble.py` exists before multiple models exist

- callers should depend on one stable interface
- adding `N-BEATS` later should not force job rewrites

## Risks And Nuances To Mention

### Scope boundary

Sprint 4 is Prophet-first only.

This sprint does not include:

- `N-BEATS`
- anomaly detection
- portfolio risk

Those are later enhancements.

### Idempotency nuance

Prediction rows are minute-bucketed for idempotency.

That means:

- a retry in the same minute maps to the same logical write
- scheduler retries should not keep duplicating prediction rows inside that minute

### Source dependency

The job depends on `yfinance`.

That means:

- DNS or Yahoo failures can break the fetch step
- the job should fail loudly rather than writing partial junk

### Container warning

Prophet may log a Plotly warning inside the worker container.

That warning does not block model training or forecast writes.
