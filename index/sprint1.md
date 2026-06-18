# Sprint 1 Interview Notes

## What Sprint 1 builds

Sprint 1 builds the scheduling core of the project:

- A small Raft cluster stores job state.
- Jobs are created through an API.
- Only the current Raft leader is allowed to fire jobs.
- Job updates and run records go through the Raft log, not direct memory writes.
- The system tries to avoid duplicate executions during leader changes.

This is the base for the rest of the platform. Later ML, data pipelines, and AI jobs all depend on this layer behaving correctly.

## The high-level architecture

The main runtime path is:

1. `cmd/crond/main.go` starts one node.
2. That node joins a Raft cluster.
3. `internal/store` sits on top of Raft and acts as the replicated state machine.
4. `internal/scheduler` periodically checks which jobs are due.
5. If the node is the leader, it advances the job schedule and records runs through the store.
6. `internal/executor` actually performs the work for the job.
7. `internal/api` exposes job CRUD and manual trigger endpoints.

The key idea is simple:

- Raft decides the order of state changes.
- The store is the only place that mutates replicated state.
- The scheduler reads replicated state and proposes more replicated state changes.

## What the store owns

The store is the most important Sprint 1 component.

It owns:

- `Job`
- `JobRun`
- `StoreState`
- `Command`

The store also owns these rules:

- Every mutation must go through `Submit()`.
- `Submit()` serializes the mutation into a `Command`.
- `Command` is appended through `raft.Start(...)`.
- Real state changes only happen inside `applyLoop()`.

This matters in interview language because it shows clean separation:

- Raft is consensus.
- Store is the deterministic state machine.
- Scheduler and API are clients of that state machine.

## Why `applyLoop()` is the center of correctness

`applyLoop()` is where committed log entries become real state.

That gives three benefits:

1. All nodes apply the same sequence of mutations in the same order.
2. Followers do not invent local state.
3. Recovery is simpler because replay and live execution use the same code path.

If asked why direct writes are dangerous, the answer is:

- A local write on one node would bypass consensus.
- That breaks the replicated state machine model.
- After failover, other nodes would not agree on job or run state.

## How duplicate runs are reduced

The project uses an idempotency key for each scheduled firing:

- `RunIdempotencyKey(jobID, scheduledTime)`

The scheduled time is truncated to the minute. That means the same logical fire for the same job produces the same key even if leadership changes or the action is retried.

The scheduler checks `AlreadyFired(key)` before running.
The store also upserts run records by `IdempotencyKey`.

This is important because a normal scheduler can accidentally double-write execution history when:

- a leader crashes after starting work
- a follower becomes leader and retries
- the final run record is replayed

## Why the scheduler updates `NextRun` before execution

This is a deliberate design decision.

Flow:

1. A due job is found.
2. The scheduler computes the next schedule.
3. The updated job is committed through Raft first.
4. Only after that does the executor run.

Reason:

- If the process dies after the schedule update but before execution finishes, the cluster still knows that this minute was already handled at the scheduling layer.
- Recovery logic can inspect the missed run and decide whether to skip or run once.

This is a tradeoff:

- It improves consistency and duplicate resistance.
- It means “schedule moved forward” and “work completed successfully” are two separate facts.

That is why `JobRun` exists separately from `Job`.

## Catch-up behavior

`ReconcileMissedJobs()` handles downtime.

It looks at jobs whose `NextRun` is already in the past and applies the job’s catch-up policy:

- `skip`: move `NextRun` forward and do not run the missed fire
- `run_once`: run one catch-up execution now, then move forward

Interview framing:

- This is operational behavior, not consensus behavior.
- Consensus preserves the job state.
- Catch-up decides business behavior after recovery.

## Why snapshots matter here

Without snapshots, the Raft log keeps growing as jobs and runs are recorded.

The store snapshots:

- the current job map
- the in-memory run slice

This reduces replay cost on restart and keeps the log from growing forever.

In this implementation, the store snapshots every committed mutation before `Submit()` returns.
That is more aggressive than a high-throughput production system would use, but it gives Sprint 1 strong restart behavior with this Raft implementation.

## Why `PartitionKey` exists now even though it is not used yet

`PartitionKey` is a forward-compatibility field.

Right now:

- every job still routes to shard `0`
- `ClusterRouter` always returns `0` unless sharding is enabled

Why add it now:

- future multi-cluster sharding can route jobs without changing the job schema
- user-scoped jobs can later hash by `user_id`

This is a good interview talking point because it shows design for growth without building the full complexity today.

## How this differs from a normal Raft scheduler

A normal Raft-backed cron scheduler often stops at:

- replicated jobs
- leader-only execution
- basic run history

This project changes the design in a few important ways.

### 1. It is designed as the control plane for a larger data/ML system

This scheduler is not the product by itself.
It is the orchestration layer for:

- data fetch jobs
- ML jobs
- AI pipeline jobs
- future Kafka-based work

That is why executors are separated cleanly and why the API contract matters early.

### 2. It adds idempotency as a first-class concern

A basic scheduler might treat run logging as simple append-only history.
This project treats retries and failover as normal events and builds deterministic run keys into the design.

That matters because downstream systems like databases, Kafka consumers, and ML workers are usually at-least-once systems too.

### 3. It adds recovery policy as product behavior

Normal cron logic only asks, “Is this due?”
This project also asks, “What should happen if the system was offline when it was due?”

That is why `CatchupPolicy` is embedded in the job model.

### 4. It separates scheduling from execution

Executors live in `internal/executor`, not inside scheduler code.

That makes it easier to grow from shell and HTTP to Kafka and later LangGraph/n8n style executors without turning the scheduler into a big switch statement with side effects everywhere.

### 5. It carries future sharding hooks early

`PartitionKey` and `ClusterRouter` are not necessary for a tiny scheduler.
They exist because the project roadmap expects the scheduling layer to eventually support more than one consensus group.

## Current limitations you should be honest about in interview

These are not failures. They are current boundaries.

- The Raft persister is now file-backed in Docker through `RAFT_DATA_DIR=/data`, but durability depends on keeping the Docker named volumes.
- The Kafka executor is only scaffolded, not fully wired to a broker library yet.
- The system reduces duplicate runs strongly, but true exactly-once execution across arbitrary external side effects is always harder than exactly-once recording.
- Run history is kept in memory in the store state machine for Sprint 1 simplicity.

That last point is a useful interview answer:

- consensus can make state ordering correct
- but external side effects still need idempotent downstream systems

## Good interview answers

### What problem is Raft solving here?

Raft gives the cluster one agreed order of mutations. That lets all nodes keep the same job state and makes leader failover predictable.

### Why not let every node run the scheduler?

Because cron by itself is not coordination. Without leader-only execution plus replicated state, multiple nodes can fire the same job.

### Why is `store.Submit()` important?

It is the gate that forces all writes through consensus. That keeps the store a proper replicated state machine instead of a shared in-memory cache with Raft attached on the side.

### Why track `JobRun` separately from `Job`?

A job definition and an execution result are different facts. The definition says what should happen. The run says what actually happened at a specific scheduled time.

### Why snapshot if the state is small?

Because the log grows with every run record. Snapshots keep replay time bounded and are the standard way to compact replicated state.

### Why update `NextRun` before calling the executor?

Because scheduling intent must survive failover. If execution crashes mid-flight, the cluster still knows that schedule progression happened and catch-up policy can resolve the remainder.

## Files to know before interview

- [cmd/crond/main.go](/Users/jay/Desktop/market-intel/cmd/crond/main.go)
- [internal/store/store.go](/Users/jay/Desktop/market-intel/internal/store/store.go)
- [internal/store/types.go](/Users/jay/Desktop/market-intel/internal/store/types.go)
- [internal/store/snapshot.go](/Users/jay/Desktop/market-intel/internal/store/snapshot.go)
- [internal/scheduler/scheduler.go](/Users/jay/Desktop/market-intel/internal/scheduler/scheduler.go)
- [internal/scheduler/recovery.go](/Users/jay/Desktop/market-intel/internal/scheduler/recovery.go)
- [internal/executor/executor.go](/Users/jay/Desktop/market-intel/internal/executor/executor.go)
- [internal/api/handlers.go](/Users/jay/Desktop/market-intel/internal/api/handlers.go)

## Short version to memorize

Sprint 1 turns Raft into a job-control plane:

- jobs are replicated through a store state machine
- only the leader schedules work
- schedule progression is committed before execution
- runs use deterministic idempotency keys
- recovery behavior is explicit through catch-up policy
- the design already carries hooks for sharding and downstream ML/data executors
