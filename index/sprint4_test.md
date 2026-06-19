# Sprint 4 Test Guide

## Purpose

This file lists the simple test scenarios for Sprint 4.

Main Sprint 4 goal:

- prove that the Prophet baseline works locally
- prove that the worker container can run prediction jobs
- prove that the scheduler can trigger `run_predictions` end to end

## Prerequisites

- Docker Desktop running
- Compose services available
- network access for `yfinance`
- local Python environment with `prophet`

## Scenario 1: Local Prophet import test

Command:

```bash
.venv/bin/python -c "import prophet; print(prophet.__version__)"
```

Expected:

- import succeeds
- a Prophet version is printed

## Scenario 2: Direct model test

Command:

```bash
MPLCONFIGDIR=/private/tmp/matplotlib YFINANCE_CACHE_DIR=/private/tmp/yfinance .venv/bin/python -c "import yfinance as yf; from ml.models.prophet_model import predict_close; df=yf.download('AAPL', period='90d', interval='1d', progress=False, auto_adjust=False); out=predict_close('AAPL', df, horizon_days=7); print(out['ticker'], out['model'], out['horizon_days'], out['direction'], out['confidence'], len(out['forecast_values']))"
```

Expected:

- `AAPL prophet 7 ...`
- 7 forecast values returned

Why this matters:

- isolates the model logic from the job, Kafka, and DB

## Scenario 3: Ensemble wrapper smoke test

Command:

```bash
.venv/bin/python -c "import pandas as pd; from ml.models.ensemble import predict; df=pd.DataFrame({'Date': pd.bdate_range('2026-01-01', periods=90), 'Close': [100 + i*0.2 for i in range(90)]}); out=predict('AAPL', df); print(out['ticker'], out['model'], out['horizon_days'], out['direction'], len(out['forecast_values']), out['component_models'])"
```

Expected:

- `model` is `ensemble`
- `component_models` includes `prophet`

## Scenario 4: Prediction job syntax test

Command:

```bash
.venv/bin/python -m py_compile ml/models/prophet_model.py ml/models/ensemble.py ml/jobs/run_predictions.py ml/db/timescale.py
```

Expected:

- no syntax errors

## Scenario 5: Direct job test

Command:

```bash
MPLCONFIGDIR=/private/tmp/matplotlib YFINANCE_CACHE_DIR=/private/tmp/yfinance TICKERS=AAPL,NVDA DB_HOST=localhost DB_PORT=5432 DB_NAME=marketintel DB_USER=marketintel DB_PASSWORD=marketintel_dev REDIS_URL=redis://localhost:6379/0 .venv/bin/python -m ml.jobs.run_predictions
```

Expected:

- job logs start
- job logs stored predictions for the requested tickers
- job logs finish successfully

Check TimescaleDB:

```bash
docker compose exec -T timescaledb psql -U marketintel -d marketintel -c "SELECT time, ticker, model, horizon_days, direction, confidence, jsonb_array_length(forecast_values) AS forecast_days FROM predictions ORDER BY time DESC, ticker ASC LIMIT 10;"
```

Expected:

- one recent row per tested ticker
- `model` is `ensemble`
- `forecast_days` is `7`

## Scenario 6: Worker image build

Command:

```bash
docker compose build ml-worker
```

Expected:

- image build succeeds
- Prophet installs
- native indicators still compile

## Scenario 7: Worker container import test

Command:

```bash
docker compose exec -T ml-worker python -c "from prophet import Prophet; import prophet; import ml.jobs.run_predictions as rp; print(prophet.__version__, rp.RunPredictionsJob.job_name, rp.DEFAULT_HISTORY_PERIOD)"
```

Expected:

- import succeeds inside container
- job name is `run_predictions`
- history default is `90d`

## Scenario 8: Container direct job test

Command:

```bash
docker compose exec -T -e MPLCONFIGDIR=/tmp/matplotlib -e TICKERS=AAPL -e DB_HOST=timescaledb -e DB_PORT=5432 -e DB_NAME=marketintel -e DB_USER=marketintel -e DB_PASSWORD=marketintel_dev -e REDIS_URL=redis://redis:6379/0 ml-worker python -m ml.jobs.run_predictions
```

Expected:

- job runs inside container
- prediction row is written into Compose TimescaleDB

## Scenario 9: Seed scheduler job

Command:

```bash
./scripts/seed_jobs.sh
```

Expected:

- `run_predictions` is created
- executor is `kafka`
- payload is `jobs.ml:{"job":"run_predictions"}`

Check:

```bash
curl http://127.0.0.1:8080/jobs/run_predictions
```

Expected:

- cron is the after-close weekday schedule
- `next_run` is set

## Scenario 10: End-to-end scheduled prediction run

Steps:

1. Start Compose.
2. Run `./scripts/init_topics.sh`.
3. Seed jobs.
4. For testing only, temporarily create `run_predictions` with a per-minute cron.
5. Wait for the next scheduler tick.

Check scheduler:

```bash
curl http://127.0.0.1:8080/jobs/run_predictions
```

Expected:

- `last_status` is `success`
- `last_run` is set

Check worker logs:

```bash
docker compose logs --since=3m ml-worker
```

Expected:

- `dispatching job=run_predictions`
- `stored prediction ticker=AAPL`

Check TimescaleDB:

```bash
docker compose exec -T timescaledb psql -U marketintel -d marketintel -c "SELECT time, ticker, model, horizon_days, direction, confidence, jsonb_array_length(forecast_values) AS forecast_days FROM predictions WHERE time >= NOW() - INTERVAL '5 minutes' ORDER BY time DESC, ticker ASC;"
```

Expected:

- fresh recent prediction rows exist
- `forecast_days` is `7`

## Scenario 11: Forecast JSON shape

Command:

```bash
docker compose exec -T timescaledb psql -U marketintel -d marketintel -c "SELECT ticker, forecast_values->0 AS first_day, forecast_values->-1 AS last_day FROM predictions ORDER BY time DESC, ticker ASC LIMIT 2;"
```

Expected:

- each forecast point contains:
  `date`, `value`, `lower`, `upper`

## Scenario 12: Idempotency behavior

Command:

```bash
docker compose exec -T timescaledb psql -U marketintel -d marketintel -c "SELECT time, ticker, model, COUNT(*) FROM predictions GROUP BY time, ticker, model ORDER BY time DESC;"
```

Expected:

- retries in the same minute do not create uncontrolled duplicates for the same logical write

Why this matters:

- confirms safe retry behavior for scheduled prediction jobs
