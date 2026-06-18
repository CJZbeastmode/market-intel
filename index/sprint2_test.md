# Sprint 2 Test Guide

## Purpose

This file lists the simple test scenarios for Sprint 2.

Main Sprint 2 goal:

- prove that a scheduled Kafka job can trigger Python data fetching and persist a quote

## Prerequisites

- Docker Desktop running
- Compose services available
- network access for `yfinance`

## Scenario 1: Build the ML worker image

Command:

```bash
docker compose build ml-worker
```

Expected:

- build finishes successfully
- no Torch/CUDA giant install path is used in this image

Why this matters:

- proves the worker container uses the smaller runtime dependency set

## Scenario 2: Start infrastructure

Command:

```bash
docker compose up -d
docker compose ps
```

Expected:

- `crond-1`, `crond-2`, `crond-3` running
- `redpanda` healthy
- `timescaledb` healthy
- `redis` healthy
- `ml-worker` running

## Scenario 3: Create Kafka topics

Command:

```bash
./scripts/init_topics.sh
```

Expected:

- `jobs.ml` exists with 6 partitions
- market topics exist with 3 partitions

Check:

```bash
docker compose exec -T redpanda rpk topic list -X brokers=redpanda:9092
```

## Scenario 4: Seed default scheduler job

Command:

```bash
./scripts/seed_jobs.sh
```

Expected:

- script finds the current API leader
- deletes old `fetch_live_quotes` job if present
- recreates `fetch_live_quotes`

Check:

```bash
curl http://127.0.0.1:8081/jobs/fetch_live_quotes
```

Expected job shape:

- executor is `kafka`
- payload is `jobs.ml:{"job":"fetch_quotes"}`

## Scenario 5: Kafka executor unit test

Command:

```bash
go test ./internal/executor
```

Expected:

- tests pass

What this covers:

- bad payload rejected
- no-broker no-op path
- producer publish path
- producer error path

## Scenario 6: Quote scraping logic only

Command:

```bash
.venv/bin/python -c 'from ml.jobs.fetch_quotes import fetch_yfinance_quote; print(fetch_yfinance_quote("AAPL"))'
```

Expected:

- returns a quote dict with price, volume, source, and time

Why this matters:

- isolates `yfinance` logic from Kafka, Redis, and DB

## Scenario 7: Worker startup test

Command:

```bash
docker compose logs --tail=120 ml-worker
```

Expected:

- worker logs `listening topic=jobs.ml`
- consumer group joins successfully
- partitions are assigned

## Scenario 8: End-to-end scheduled fetch

Steps:

1. Start Compose.
2. Run `./scripts/init_topics.sh`.
3. Run `./scripts/seed_jobs.sh`.
4. Wait up to 90 seconds for the next cron tick.

Check scheduler:

```bash
curl http://127.0.0.1:8081/jobs/fetch_live_quotes
```

Expected:

- `last_status` is `success`
- `last_run` is set

Check worker logs:

```bash
docker compose logs --tail=120 ml-worker
```

Expected:

- `dispatching job=fetch_quotes`
- `stored quote ticker=AAPL`

Check TimescaleDB:

```bash
docker compose exec -T timescaledb psql -U marketintel -d marketintel -c "SELECT time, ticker, price, volume, source FROM market_quotes WHERE ticker='AAPL' ORDER BY time DESC LIMIT 5;"
```

Expected:

- at least one `AAPL` row exists

Check Redis:

```bash
docker compose exec -T redis redis-cli GET quote:default:AAPL
```

Expected:

- latest quote JSON exists

## Scenario 9: Rebuild scheduler after Go dependency changes

Command:

```bash
docker compose up -d --build crond-1 crond-2 crond-3
```

Expected:

- scheduler images rebuild cleanly
- no `go.sum` missing error

Why this matters:

- Sprint 2 added Kafka Go dependencies, so Docker build must copy `go.sum`

## Scenario 10: Leader-aware seeding

Command:

```bash
curl http://127.0.0.1:8080/cluster
curl http://127.0.0.1:8081/cluster
curl http://127.0.0.1:8082/cluster
```

Expected:

- exactly one node reports `"is_leader": true`

Then:

```bash
./scripts/seed_jobs.sh
```

Expected:

- script chooses the leader automatically

## Failure Scenarios To Test Later

- stop `ml-worker`, trigger the scheduler job, confirm Kafka message stays pending
- break TimescaleDB credentials, confirm worker job fails and offset is not committed
- set `DEMO_MODE=true`, confirm fake quotes still flow through Redis and TimescaleDB
- scale `ml-worker` replicas later and confirm `jobs.ml` partitions spread across workers

## What Passed In This Repo

The successful Sprint 2 end-to-end result was:

- scheduler run status: `success`
- worker dispatched `fetch_quotes`
- worker stored `AAPL`
- TimescaleDB row inserted for `AAPL`
- Redis latest quote key populated for `AAPL`
