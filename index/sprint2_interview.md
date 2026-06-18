# Sprint 2 Interview Notes

## Goal

Sprint 2 turns the Raft scheduler from a pure local scheduler into a small distributed data pipeline.

The full flow is now:

1. Go Raft scheduler decides when a job should run.
2. Kafka executor publishes a job message into Redpanda.
3. Python `ml-worker` consumes that message.
4. Python job code fetches data and writes it to Redis and TimescaleDB.

This is the first sprint where the scheduler is not the end of the story. It becomes the control plane.

## What Was Built

### 1. TimescaleDB + Redis + Redpanda

- TimescaleDB stores historical quotes and later analytics.
- Redis stores the freshest quote for fast reads.
- Redpanda is the message bus between Go and Python.

This matters because Go should not directly do all ML and data work. The scheduler should trigger work, not become a giant worker process.

### 2. Database schema

Main table for this sprint:

- `market_quotes`

Important columns:

- `time`
- `user_id`
- `ticker`
- `price`
- `volume`
- `source`
- `idempotency_key`

Important idea:

- writes use `idempotency_key` so repeated processing does not create duplicate rows for the same logical quote write

### 3. Python worker

File:

- [ml/worker.py](/Users/jay/Desktop/market-intel/ml/worker.py)

What it does:

- listens to Kafka topic `jobs.ml`
- reads JSON like `{"job":"fetch_quotes"}`
- turns that into a Python module name like `ml.jobs.fetch_quotes`
- runs that job
- commits Kafka offset only after success

Why this is important:

- failed jobs can be retried
- Go scheduler stays simple
- Python side can grow independently

### 4. Base job and quote fetch job

Files:

- [ml/jobs/_base.py](/Users/jay/Desktop/market-intel/ml/jobs/_base.py)
- [ml/jobs/fetch_quotes.py](/Users/jay/Desktop/market-intel/ml/jobs/fetch_quotes.py)

`BaseJob` gives every Python job:

- logger
- user id
- Timescale client
- Redis client

`fetch_quotes.py`:

- reads tickers from Kafka payload or env
- fetches live data from `yfinance`
- writes latest quote to TimescaleDB
- writes latest quote to Redis

### 5. Go Kafka executor

File:

- [internal/executor/kafka.go](/Users/jay/Desktop/market-intel/internal/executor/kafka.go)

Before Sprint 2:

- Kafka executor returned `not implemented`

After Sprint 2:

- it parses `topic:message`
- connects to Kafka
- publishes the message

Example:

```text
jobs.ml:{"job":"fetch_quotes"}
```

becomes:

- topic: `jobs.ml`
- message: `{"job":"fetch_quotes"}`

### 6. Topic setup and seed job scripts

Files:

- [scripts/init_topics.sh](/Users/jay/Desktop/market-intel/scripts/init_topics.sh)
- [scripts/seed_jobs.sh](/Users/jay/Desktop/market-intel/scripts/seed_jobs.sh)

`init_topics.sh`:

- creates Kafka topics
- `jobs.ml` gets 6 partitions

`seed_jobs.sh`:

- finds current leader API
- creates `fetch_live_quotes`

The seeded job means:

```json
{
  "name": "fetch_live_quotes",
  "executor": "kafka",
  "payload": "jobs.ml:{\"job\":\"fetch_quotes\"}"
}
```

So the Go scheduler does not fetch quotes itself. It schedules a Kafka message that tells Python to do it.

## Example Workflow

Concrete example with `AAPL`:

1. Scheduler reaches the next minute tick.
2. Job `fetch_live_quotes` becomes due.
3. Go scheduler records the run in its own store.
4. Kafka executor publishes `{"job":"fetch_quotes"}` to `jobs.ml`.
5. Python worker consumes that message.
6. `ml.jobs.fetch_quotes` runs.
7. `yfinance` fetches `AAPL`.
8. TimescaleDB gets a new `market_quotes` row.
9. Redis gets `quote:default:AAPL`.

## What Changed Compared To A Normal Raft Scheduler

Normal Raft scheduler:

- stores jobs
- elects leader
- runs local executors like shell or HTTP

This project after Sprint 2:

- still uses Raft for exactly-one scheduler decision
- but real data work is pushed to Kafka and Python workers

Why this is different:

1. Execution is now asynchronous.
   The scheduler marks the job successful after publish, not after the full ML/data pipeline finishes.

2. The scheduler is now a control plane.
   It decides *when* work should start, but another system does the heavy work.

3. We split hot and cold storage.
   Redis is for current value. TimescaleDB is for history.

4. We added message-bus scaling.
   More Python workers can be added later without changing scheduler logic.

5. We added idempotent write thinking.
   In a plain scheduler, success might just mean shell command exit code.
   Here we also care about duplicate Kafka delivery and duplicate DB writes.

## Important Design Reasons

### Why Go does not fetch quotes directly

- `yfinance` and later ML libraries live better in Python
- keeps Go scheduler small and stable
- easier to scale Python workers separately

### Why Kafka is between Go and Python

- decouples scheduler from worker runtime
- jobs can queue if workers are busy
- lets many workers share one job stream later

### Why Redis and TimescaleDB both exist

- Redis is fast for latest quote reads
- TimescaleDB is correct for historical queries and analytics

### Why `jobs.ml` has 6 partitions

- allows multiple workers later
- gives room for parallel job consumption

### Why `fetch_live_quotes` and `fetch_quotes` are different names

- `fetch_live_quotes` is the scheduler job name
- `fetch_quotes` is the Python worker module name

This separation is useful:

- scheduler names describe *business jobs*
- Python names describe *implementation modules*

## Failure Model You Should Be Ready To Explain

### Case 1: Go publish fails

- scheduler run fails
- run record shows failure
- nothing reaches Python worker

### Case 2: Kafka publish succeeds but Python job fails

- scheduler run may still show success because publish succeeded
- worker leaves Kafka offset uncommitted
- message can be retried

This is one of the biggest interview points:

- scheduler success and full pipeline success are no longer the same thing

### Case 3: duplicate message

- worker may see the same logical work again
- DB idempotency key reduces duplicate writes

## What We Verified

Successful end-to-end test proved:

1. Go scheduler fired the cron job.
2. Kafka executor published to `jobs.ml`.
3. Python worker consumed and ran `fetch_quotes`.
4. `AAPL` quote was written to TimescaleDB.
5. latest `AAPL` quote was cached in Redis.

## Interview One-Liners

- “Sprint 2 turns the Raft scheduler into the control plane for a distributed market-data pipeline.”
- “Go decides when work runs; Python does the market-data work.”
- “Kafka is the handoff layer between scheduler decisions and ML/data execution.”
- “Redis stores latest values, TimescaleDB stores history.”
- “The worker commits Kafka offsets only after successful job execution.”
- “A scheduler success now means publish success, not necessarily end-to-end business success.”
