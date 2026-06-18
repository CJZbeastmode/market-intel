# Sprint 1 Test Guide

## Purpose

This document is the simple test checklist for Sprint 1.

Sprint 1 scope:

- Raft-backed job store
- leader-only scheduling
- API job CRUD
- manual trigger
- missed-job catch-up
- duplicate protection for job runs

## Fast checks

Run these first:

```bash
go test ./...
docker compose config --quiet
```

What they cover:

- Go unit and package tests pass
- Docker Compose file parses correctly

## Scenario 1: Unit tests

Command:

```bash
go test ./...
```

What this validates:

- store create, update, delete
- run recording
- run history cap
- snapshot encode/apply
- scheduler due-job firing
- catch-up logic
- executor behavior for shell, HTTP, Kafka placeholder
- API handlers
- existing Raft tests in the repo

If this fails:

- fix unit failures before trying cluster tests

## Scenario 2: Start the 3-node cluster

Command:

```bash
docker compose up --build
```

What to expect:

- three `crond-*` services start
- one node becomes leader
- the others remain followers
- all three nodes expose APIs: `localhost:8080`, `localhost:8081`, `localhost:8082`

Useful checks:

```bash
curl http://localhost:8080/cluster
curl http://localhost:8081/cluster
curl http://localhost:8082/cluster
docker compose ps
docker compose logs crond-1
docker compose logs crond-2
docker compose logs crond-3
```

What to look for:

- `/cluster` returns JSON with `term` and `is_leader`
- logs show a leader election settling
- one node logs catch-up reconciliation after leadership

## Scenario 3: Create a job through the API

Command:

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name":"test-shell",
    "cron_expr":"* * * * *",
    "executor":"shell",
    "payload":"echo hello from sprint1"
  }'
```

Then list jobs:

```bash
curl http://localhost:8080/jobs
```

What this validates:

- API accepts valid job input
- cron expression is parsed
- defaults are applied:
  `catchup_policy=skip`, `partition_key=default`, `enabled=true`
- job is written through the Raft-backed store

Expected result:

- `POST /jobs` returns `201`
- `GET /jobs` returns the new job
- job has `next_run`, `created_at`, `updated_at`

## Scenario 4: Invalid job input

Command:

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name":"bad-job",
    "cron_expr":"not-a-cron",
    "executor":"shell",
    "payload":"echo hi"
  }'
```

Expected result:

- HTTP `400`
- error mentions invalid `cron_expr`

What this validates:

- API rejects invalid schedules before they reach the store

## Scenario 5: Job fires automatically on schedule

Create a minutely job:

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name":"minute-job",
    "cron_expr":"* * * * *",
    "executor":"shell",
    "payload":"echo auto-run"
  }'
```

Wait about 70 seconds, then fetch job detail:

```bash
curl http://localhost:8080/jobs
```

Get the job id, then:

```bash
curl http://localhost:8080/jobs/<JOB_ID>
```

What this validates:

- leader-only scheduler loop is active
- due jobs are detected
- `NextRun` moves forward
- a `JobRun` record appears

Expected result:

- at least one run is present
- run status becomes `success` for a simple shell job

## Scenario 6: Manual trigger

Command:

```bash
curl -X POST http://localhost:8080/jobs/<JOB_ID>/trigger
curl http://localhost:8080/jobs/<JOB_ID>
```

What this validates:

- manual trigger path works
- scheduler can fire a job immediately outside normal cron timing
- run history is updated

Expected result:

- trigger endpoint returns `202`
- a new run eventually appears in job detail

## Scenario 7: Delete a job

Command:

```bash
curl -X DELETE http://localhost:8080/jobs/<JOB_ID>
curl http://localhost:8080/jobs/<JOB_ID>
```

Expected result:

- delete returns `204`
- follow-up get returns `404`

What this validates:

- job delete path works through the store
- deleted jobs are removed from read APIs

## Scenario 8: Leader endpoint behavior

Each node exposes its own API port.

Check leader info:

```bash
curl http://localhost:8080/cluster
curl http://localhost:8081/cluster
curl http://localhost:8082/cluster
```

What this validates:

- cluster status endpoint is wired
- operator can inspect current term and identify which API node is leader

## Scenario 9: Snapshot path

This is mainly covered by unit tests:

```bash
go test ./internal/store -v
```

What this validates:

- snapshot encoding from store state
- snapshot restore into a fresh store
- snapshot application through `ApplyMsg`

Why this is enough for Sprint 1:

- snapshots are store-level state compaction
- unit tests already hit the actual encode/apply logic directly

## Scenario 10: Catch-up policy `skip`

This scenario is mostly unit-tested in `internal/scheduler`.

Command:

```bash
go test ./internal/scheduler -v
```

What this validates:

- if `NextRun` is in the past and policy is `skip`
- scheduler moves `NextRun` forward
- scheduler does not fire executor work

Manual note:

- this can now be tested end-to-end because Raft state is file-backed under `/data`
- do not delete Docker volumes during the test, or the persisted state is intentionally removed

## Scenario 11: Catch-up policy `run_once`

Command:

```bash
go test ./internal/scheduler -v
```

What this validates:

- if a job was missed and policy is `run_once`
- scheduler fires it once during reconciliation
- `NextRun` advances after catch-up

Manual note:

- same persistence limitation applies here

## Scenario 12: Duplicate protection in store

Command:

```bash
go test ./internal/store -v
```

What this validates:

- run updates with the same idempotency key are merged
- run history is bounded
- store logic does not append duplicate logical runs

Why this matters:

- during failover, the same logical scheduled minute can be retried
- the store should treat that as the same run, not a new one

## Scenario 13: Failover while cluster stays alive

This is the most important real integration test.

1. Start the cluster:

```bash
docker compose up --build -d
```

2. Create a minutely job:

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name":"failover-test",
    "cron_expr":"* * * * *",
    "executor":"shell",
    "payload":"echo failover-test"
  }'
```

3. Wait until it has fired once.

4. Stop one node:

```bash
docker compose stop crond-1
```

5. Check the remaining logs:

```bash
docker compose logs crond-2
docker compose logs crond-3
```

What this validates:

- a new leader can be elected while the cluster stays up
- scheduling continues on the surviving quorum

After stopping one node, query the remaining API ports:

```bash
curl http://localhost:8081/cluster
curl http://localhost:8082/cluster
```

This should let you find the new leader from the host.

## Scenario 14: Full restart durability

This should now be tested because each node uses `RAFT_DATA_DIR=/data`.

What to test:

1. create jobs
2. restart the entire cluster
3. verify jobs and run history still exist

Command sketch:

```bash
docker compose restart
sleep 10
curl http://localhost:8080/jobs
curl http://localhost:8081/jobs
curl http://localhost:8082/jobs
```

Expected result:

- at least the elected leader should return the previously created jobs
- non-leaders may return `409` for writes, but reads should expose local replicated state after replay

## Scenario 15: API conflict on non-leader

The store returns `ErrNotLeader`, and the API maps that to HTTP `409`.

This is covered best by unit tests:

```bash
go test ./internal/api -v
```

What this validates:

- leader-aware error handling is present
- clients get a clear retry signal

## Scenario 16: Executor-specific tests

Shell executor:

```bash
go test ./internal/executor -run Shell -v
```

HTTP executor:

```bash
go test ./internal/executor -run HTTP -v
```

Kafka executor:

```bash
go test ./internal/executor -run Kafka -v
```

What these validate:

- shell commands run and timeout correctly
- HTTP jobs send POST requests and reject non-2xx
- Kafka placeholder validates payload shape and safely no-ops without brokers

## Recommended test order

Use this order when validating changes:

1. `go test ./...`
2. `docker compose config --quiet`
3. `docker compose up --build`
4. `GET /cluster`
5. create job
6. list jobs
7. wait for automatic run
8. manual trigger
9. delete job
10. optional failover logs test

## Known limits while testing

- Full restart durability depends on the named Docker volumes not being deleted.
- There is no external database yet for persistent run history.
- Kafka execution is scaffolded but not fully implemented.

## Pass criteria for current Sprint 1 code

Treat Sprint 1 as healthy if all of these are true:

- `go test ./...` passes
- Compose config parses
- cluster starts
- leader is elected
- job create/list/get/delete works
- minutely shell job fires
- manual trigger works
- run records appear with final status
- unit tests for snapshots, catch-up, and dedupe all pass
