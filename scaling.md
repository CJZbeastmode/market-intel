# SCALING.md — Market Intel Platform
## Scalability Design Decisions & Implementation Guide

> This document is a companion to FORWARD.md. It covers the three architectural
> decisions that are cheap to implement now but expensive to change later, the three
> scaling ceilings the system will hit as it grows, and the exact code patterns that
> keep every ceiling a config change rather than a rewrite.
>
> Current target: personal use, 1 user, 20 tickers, ~50 jobs/day.
> Designed for: open source release, 1000+ users, horizontal scaling without
> structural changes.

---

## The Core Principle

Build the hooks now. Ignore them until you need them.

Every scalability decision in this document follows the same pattern: add one field,
one column, one parameter, or one abstraction layer today — hardcode the trivial
value — and flip it to real logic later without touching any other file. The cost
now is near zero. The cost of retrofitting later is a full schema migration or a
rewrite of the routing layer.

---

## Decision 1 — Multi-Tenancy via `user_id` (Do This in Sprint 1)

### The problem it solves

Every table in the system currently serves one person. When you open source this,
100 users running the same Docker Compose share nothing — each has their own
TimescaleDB instance. That's fine. But if you ever want a hosted version, a shared
deployment, or simply to run multiple portfolio profiles yourself, every table needs
a tenant boundary.

Adding `user_id` after millions of rows exist means a full table scan migration,
index rebuilds, and query rewrites across every job file. Doing it now costs one
column per table and one `WHERE user_id = 'default'` per query.

### What to do now

Every TimescaleDB table gets `user_id TEXT NOT NULL DEFAULT 'default'`. This is
already in `FORWARD.md`'s `init.sql`. Do not remove it. Do not change the default.

Every Redis key is namespaced: `quote:{user_id}:{ticker}` not `quote:{ticker}`.

Every Qdrant payload includes `user_id` in metadata. Every query filters on it.

Every MinIO object path includes user_id: `briefs/{user_id}/{date}/daily_brief.txt`.

### Implementation pattern (Python jobs)

```python
# ml/jobs/_base.py
# user_id is read from environment — 'default' for personal use,
# real UUID for multi-tenant deployment

import os

class BaseJob:
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.user_id = os.getenv('USER_ID', 'default')
        # All DB queries use self.user_id in WHERE clause — never omit it

# CORRECT — always filter by user_id
def get_latest_quote(self, ticker: str) -> dict:
    return self.db.query_one("""
        SELECT price, time FROM market_quotes
        WHERE user_id = %s AND ticker = %s
        ORDER BY time DESC LIMIT 1
    """, (self.user_id, ticker))

# WRONG — never query without user_id
def get_latest_quote_wrong(self, ticker: str) -> dict:
    return self.db.query_one("""
        SELECT price, time FROM market_quotes
        WHERE ticker = %s
        ORDER BY time DESC LIMIT 1
    """, (ticker,))
```

### Implementation pattern (Redis keys)

```python
# ml/db/redis_client.py
# All keys are namespaced by user_id — never use bare ticker as key

class RedisClient:
    def __init__(self):
        self.client = redis.from_url(os.getenv('REDIS_URL', 'redis://redis:6379/0'))
        self.user_id = os.getenv('USER_ID', 'default')

    def quote_key(self, ticker: str) -> str:
        return f"quote:{self.user_id}:{ticker}"

    def brief_key(self) -> str:
        return f"brief:{self.user_id}:latest"

    def alert_key(self, ticker: str) -> str:
        return f"alert:{self.user_id}:{ticker}"

    def set_quote(self, ticker: str, data: dict, ttl: int = 120):
        self.client.setex(self.quote_key(ticker), ttl, json.dumps(data))

    def get_quote(self, ticker: str) -> dict | None:
        raw = self.client.get(self.quote_key(ticker))
        return json.loads(raw) if raw else None
```

### Implementation pattern (Qdrant)

```python
# ml/rag/embedder.py
# Every stored point includes user_id in payload
# Every query filters on user_id

def embed_and_store(self, doc_id: str, text: str, metadata: dict):
    # metadata must include user_id — default injected if missing
    metadata.setdefault('user_id', os.getenv('USER_ID', 'default'))
    # ... rest of embed_and_store as in FORWARD.md

def query_qdrant(self, query: str, **kwargs) -> list[dict]:
    user_id = os.getenv('USER_ID', 'default')
    must_conditions = [
        FieldCondition(key='user_id', match=MatchValue(value=user_id))
    ]
    # ... append additional filters from kwargs
```

### Implementation pattern (MinIO paths)

```python
# All MinIO keys include user_id as first path component
# This allows per-user bucket policies later without moving objects

def brief_key(date: str) -> str:
    user_id = os.getenv('USER_ID', 'default')
    return f"briefs/{user_id}/{date}/daily_brief.txt"

def model_key(ticker: str, model_name: str) -> str:
    user_id = os.getenv('USER_ID', 'default')
    return f"models/{user_id}/{ticker}/{model_name}/latest.pkl"

def report_key(report_type: str, date: str) -> str:
    user_id = os.getenv('USER_ID', 'default')
    return f"reports/{user_id}/{report_type}/{date}.txt"
```

### What changes when you go multi-tenant

Only two things: `USER_ID` env var gets a real UUID per user, and the API layer
adds a JWT middleware that sets `USER_ID` in the request context. Every job, query,
and Redis key already uses it. Zero schema migration, zero query rewrites.

---

## Decision 2 — Raft Sharding via `PartitionKey` (Do This in Sprint 1)

### The problem it solves

A single Raft cluster has a hard throughput ceiling. Every job submission requires
a round-trip quorum — roughly 5–20ms per commit depending on network. At personal
scale (~50 jobs/day) this is irrelevant. At open source scale with 10,000 users
each submitting jobs, the single cluster becomes a bottleneck.

The solution is consistent hashing across multiple independent Raft clusters. Each
cluster owns a partition of the job namespace. Routing is a one-line hash operation.

Adding this field after the system is deployed means a migration of the jobs table,
a rewrite of the submission API, and a resharding of every existing job. Adding it
now costs one struct field and one unused parameter.

### What to do now

Add `PartitionKey` to the `Job` struct. Always set it. Always ignore it for routing.
The router function exists but always returns cluster 0.

```go
// internal/store/types.go
// PartitionKey is set by the caller, ignored by the router until sharding is enabled.
// Convention: use user_id as partition key for user-scoped jobs,
// use "system" for infrastructure jobs.

type Job struct {
    ID            string            `json:"id"`
    Name          string            `json:"name"`
    CronExpr      string            `json:"cron_expr"`
    Executor      string            `json:"executor"`
    Payload       string            `json:"payload"`
    Enabled       bool              `json:"enabled"`
    CatchupPolicy string            `json:"catchup_policy"`
    PartitionKey  string            `json:"partition_key"` // set always, routes later
    CreatedAt     time.Time         `json:"created_at"`
    UpdatedAt     time.Time         `json:"updated_at"`
    Metadata      map[string]string `json:"metadata"`
}
```

### The router (trivial now, real later)

```go
// internal/store/router.go
// ClusterRouter maps a partition key to a Raft cluster index.
// Today: always returns cluster 0 (single cluster).
// Later: consistent hash across N clusters.

package store

import (
    "hash/fnv"
)

type ClusterRouter struct {
    numClusters int
    enabled     bool
}

func NewClusterRouter(numClusters int, enabled bool) *ClusterRouter {
    return &ClusterRouter{
        numClusters: numClusters,
        enabled:     enabled,
    }
}

// Route returns the cluster index for a given partition key.
// When sharding is disabled, always returns 0.
func (r *ClusterRouter) Route(partitionKey string) int {
    if !r.enabled || r.numClusters <= 1 {
        return 0 // single cluster — all jobs go here
    }
    // Consistent hash — same key always maps to same cluster
    h := fnv.New32a()
    h.Write([]byte(partitionKey))
    return int(h.Sum32()) % r.numClusters
}
```

### How to set PartitionKey when creating jobs

```go
// internal/api/handlers.go
// When a job is submitted via API, set PartitionKey to the authenticated user_id.
// For system jobs (seeded via seed_jobs.sh), use "system".

func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
    var job store.Job
    json.NewDecoder(r.Body).Decode(&job)

    // Set partition key from auth context (user_id for now = "default")
    userID := r.Context().Value("user_id").(string)
    if job.PartitionKey == "" {
        job.PartitionKey = userID
    }

    clusterIdx := h.router.Route(job.PartitionKey)
    // clusterIdx is always 0 today — will route to different stores when sharding enabled
    err := h.stores[clusterIdx].CreateJob(job)
    // ...
}
```

### Config flag to enable sharding later

```yaml
# config.yaml
cluster:
  mode: 'single'           # 'single' or 'distributed'
  sharding_enabled: false  # flip to true when adding cluster 1 and 2
  num_shards: 1            # increase when deploying additional Raft clusters
  election_timeout_ms: 300
```

### What changes when you enable sharding

Set `sharding_enabled: true` and `num_shards: 3` in config.yaml. Deploy two
additional Raft clusters (crond-4/5/6, crond-7/8/9). The router starts hashing.
No code changes. No schema migration. Existing jobs on cluster 0 stay on cluster 0
— their PartitionKey hashes still map to 0 until you rebalance.

---

## Decision 3 — ML Compute Decoupled from Scheduler (Do This in Sprint 2)

### The problem it solves

If ML jobs run as shell executor jobs on the scheduler node, CPU-heavy work (Prophet
training, N-BEATS inference, LangGraph pipelines) shares resources with the Raft
tick loop. Under load, the scheduler starts missing heartbeat deadlines, triggering
false leader elections, and firing jobs twice.

The fix is to decouple compute from scheduling. The scheduler publishes to a Kafka
topic. ML workers consume and execute. The scheduler never touches CPU-heavy work.

This is already partially addressed by using the `kafka` executor for ML jobs in
the job catalogue. This section makes the pattern explicit and ensures it is applied
consistently from Sprint 2 onward.

### The rule

Any job that runs a Python ML script, a LangGraph pipeline, or a Prophet/N-BEATS
model must use the `kafka` executor, never the `shell` executor.

```
shell executor  → lightweight ops only: fetch_ohlcv, fetch_news, check_price_alerts
kafka executor  → all ML compute: compute_indicators, run_predictions, daily_brief
langgraph executor → complex AI pipelines: detect_anomalies, portfolio_risk
```

### The ML worker consumer

```python
# ml/worker.py
# Runs as a separate container (ml-worker in docker-compose.yml).
# Consumes from jobs.ml topic and dispatches to the correct job handler.
# This is the ONLY process that runs CPU-heavy ML work.

import json
import logging
import os
from kafka import KafkaConsumer
from ml.jobs import (
    fetch_quotes, compute_indicators, run_predictions,
    fetch_news, detect_anomalies
)

logger = logging.getLogger('ml-worker')

JOB_HANDLERS = {
    'fetch_quotes':       fetch_quotes.FetchQuotesJob,
    'compute_indicators': compute_indicators.ComputeIndicatorsJob,
    'run_predictions':    run_predictions.RunPredictionsJob,
    'fetch_news':         fetch_news.FetchNewsJob,
    'detect_anomalies':   detect_anomalies.DetectAnomaliesJob,
}

def main():
    consumer = KafkaConsumer(
        'jobs.ml',
        bootstrap_servers=os.getenv('KAFKA_BROKERS', 'redpanda:9092'),
        group_id='ml-workers',
        value_deserializer=lambda m: json.loads(m.decode()),
        auto_offset_reset='earliest',
        enable_auto_commit=False,   # manual commit — at-least-once guarantee
        max_poll_interval_ms=600000 # 10 min — ML jobs can be slow
    )

    logger.info("ML worker started, consuming from jobs.ml")

    for message in consumer:
        envelope = message.value
        job_name = envelope.get('payload', {}).get('job', '')
        idempotency_key = envelope.get('job_id', '') + ':' + envelope.get('triggered_at', '')

        if job_name not in JOB_HANDLERS:
            logger.warning(f"Unknown job: {job_name}")
            consumer.commit()
            continue

        # Idempotency check — skip if already processed
        if _already_processed(idempotency_key):
            logger.info(f"Skipping duplicate: {idempotency_key}")
            consumer.commit()
            continue

        try:
            logger.info(f"Executing job: {job_name}")
            JOB_HANDLERS[job_name]().run()
            _mark_processed(idempotency_key)
            consumer.commit()
        except Exception as e:
            logger.error(f"Job failed: {job_name} — {e}", exc_info=True)
            # Do NOT commit — message will be redelivered after max_poll_interval_ms
            # This gives at-least-once execution with retry on failure

def _already_processed(key: str) -> bool:
    from ml.db.redis_client import RedisClient
    return RedisClient().client.exists(f"processed:{key}") == 1

def _mark_processed(key: str):
    from ml.db.redis_client import RedisClient
    # TTL of 24 hours — longer than any realistic duplicate window
    RedisClient().client.setex(f"processed:{key}", 86400, '1')

if __name__ == '__main__':
    main()
```

### Scaling the worker pool horizontally

Because workers are stateless Kafka consumers in the same consumer group, scaling
is one line in docker-compose.yml:

```yaml
# docker-compose.yml — scale ML workers without touching any other service
ml-worker:
  build:
    context: .
    dockerfile: Dockerfile.ml
  deploy:
    replicas: 3   # Kafka distributes partitions across all 3 workers automatically
  # ... rest of config unchanged
```

Kafka distributes `jobs.ml` partitions across all workers in the group. No
coordination needed. No code changes. Add replicas to add throughput.

### Redpanda topic configuration

```bash
# Run once on first startup — sets partition count high enough for horizontal scaling
# scripts/init_topics.sh

REDPANDA="redpanda:9092"

rpk topic create market.quotes     --partitions 6  --replicas 1 -b $REDPANDA
rpk topic create market.news       --partitions 3  --replicas 1 -b $REDPANDA
rpk topic create market.earnings   --partitions 3  --replicas 1 -b $REDPANDA
rpk topic create jobs.ml           --partitions 6  --replicas 1 -b $REDPANDA
rpk topic create ml.predictions    --partitions 3  --replicas 1 -b $REDPANDA
rpk topic create cron.completed    --partitions 3  --replicas 1 -b $REDPANDA
rpk topic create synthetic.events  --partitions 3  --replicas 1 -b $REDPANDA

echo "Topics created."
```

Partition counts are set high now. You cannot increase partitions without
rebalancing consumers. Setting them at 6 for high-throughput topics costs nothing
at personal scale and avoids a painful repartitioning operation later.

---

## Decision 4 — Qdrant HNSW Parameters (Do This in Sprint 5)

### The problem it solves

Qdrant's default HNSW parameters are conservative. At 500k+ vectors (90 days of
news + SEC filings + earnings transcripts for 20 tickers), default parameters give
acceptable but not optimal recall. The parameters cannot be changed after collection
creation without recreating the collection and re-embedding everything.

This is a 3-line decision made once. Set it explicitly. Never revisit.

### What to do

```python
# ml/rag/embedder.py — collection creation (called once, at first startup)

from qdrant_client.models import (
    VectorParams, Distance,
    HnswConfigDiff, OptimizersConfigDiff
)

def ensure_collection():
    collections = [c.name for c in client.get_collections().collections]
    if COLLECTION in collections:
        return  # already exists — do not recreate

    client.create_collection(
        collection_name=COLLECTION,
        vectors_config=VectorParams(
            size=384,              # all-MiniLM-L6-v2 output dimension — do not change
            distance=Distance.COSINE
        ),
        hnsw_config=HnswConfigDiff(
            m=16,                  # connections per layer — 16 balances speed and recall
                                   # increase to 32 only if recall drops below 0.9
            ef_construct=100,      # build-time quality — higher = better recall, slower index
                                   # 100 is the right value for financial text retrieval
            full_scan_threshold=10000  # switch to full scan below this count
        ),
        optimizers_config=OptimizersConfigDiff(
            indexing_threshold=20000,  # start indexing after 20k vectors
                                       # below this, brute force is faster
            memmap_threshold=50000     # mmap to disk above 50k vectors
        )
    )

    # Payload indexes — required for filtered search performance
    # Without these, every filtered query does a full collection scan
    client.create_payload_index(COLLECTION, 'ticker',   'keyword')
    client.create_payload_index(COLLECTION, 'doc_type', 'keyword')
    client.create_payload_index(COLLECTION, 'user_id',  'keyword')
    client.create_payload_index(COLLECTION, 'date',     'datetime')

    logger.info(f"Created Qdrant collection '{COLLECTION}' with explicit HNSW config")
```

### Why these specific values

`m=16` — each node connects to 16 neighbours per layer. Below 8 gives poor recall.
Above 32 increases memory usage significantly with marginal recall gain. 16 is
the standard production value for text retrieval.

`ef_construct=100` — higher values give better index quality but slower ingestion.
100 is the correct tradeoff for a batch ingestion pattern (news arrives every 30
minutes, not streaming). Reduce to 50 only if ingestion becomes a bottleneck.

`indexing_threshold=20000` — below 20k vectors, brute-force cosine similarity is
faster than HNSW traversal. The threshold prevents premature indexing on an empty
collection.

Payload indexes on `ticker`, `doc_type`, `user_id`, `date` — without these, every
filtered search (`ticker='AAPL' AND date > '2026-01-01'`) scans the full collection.
With them, Qdrant pre-filters to the matching payload subset before vector search.
At 500k vectors this difference is 200ms vs 2ms.

---

## Decision 5 — TimescaleDB Compression and Retention (Do This in Sprint 2)

### The problem it solves

1-minute quote data for 20 tickers is ~29,000 rows/day, ~10M rows/year. Without
compression, query performance degrades and disk usage grows linearly. TimescaleDB
chunk compression gives 10–20x compression on time-series data with zero query
changes — compressed chunks are transparent to SQL queries.

Retention policies automatically drop old data you don't need — 1-minute quotes
from 2 years ago have no analytical value.

### What to do

Add to `deployment/sql/init.sql` after table creation:

```sql
-- ── Compression policies ──────────────────────────────────────────────────
-- Compress chunks older than the threshold.
-- Compressed chunks are read-only but fully queryable via standard SQL.

ALTER TABLE market_quotes SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time DESC',
    timescaledb.compress_segmentby = 'user_id, ticker'
);
SELECT add_compression_policy('market_quotes', INTERVAL '7 days');

ALTER TABLE ohlcv SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time DESC',
    timescaledb.compress_segmentby = 'user_id, ticker'
);
SELECT add_compression_policy('ohlcv', INTERVAL '30 days');

ALTER TABLE indicators SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time DESC',
    timescaledb.compress_segmentby = 'user_id, ticker'
);
SELECT add_compression_policy('indicators', INTERVAL '7 days');

-- ── Retention policies ────────────────────────────────────────────────────
-- Drop data older than the threshold entirely.
-- 1-minute quotes: keep 90 days (enough for ML training)
-- Daily OHLCV: keep 10 years (needed for long-term backtesting)
-- Indicators: keep 90 days (recomputable from OHLCV)
-- Job runs: keep 30 days (operational history)

SELECT add_retention_policy('market_quotes', INTERVAL '90 days');
SELECT add_retention_policy('indicators',    INTERVAL '90 days');
-- ohlcv: no retention policy — keep forever
-- predictions: no retention policy — keep forever (small table)

-- ── Continuous aggregates ─────────────────────────────────────────────────
-- Pre-aggregate 1-minute quotes into hourly OHLCV.
-- Refreshes automatically. Queries against this view are near-instant.

CREATE MATERIALIZED VIEW quotes_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    user_id,
    ticker,
    first(price, time) AS open,
    max(price)         AS high,
    min(price)         AS low,
    last(price, time)  AS close,
    sum(volume)        AS volume
FROM market_quotes
GROUP BY bucket, user_id, ticker
WITH NO DATA;

SELECT add_continuous_aggregate_policy('quotes_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

-- Index on the materialized view for fast dashboard queries
CREATE INDEX ON quotes_hourly (user_id, ticker, bucket DESC);
```

### Query pattern for the dashboard

```python
# ml/db/timescale.py
# Dashboard queries should always prefer the continuous aggregate view
# for historical data — it's orders of magnitude faster than querying raw quotes

def get_price_history(self, ticker: str, hours: int = 24) -> list[dict]:
    """
    Use continuous aggregate for history queries.
    Use raw market_quotes only for the last 2 hours (not yet aggregated).
    """
    if hours > 2:
        return self.query("""
            SELECT bucket AS time, open, high, low, close, volume
            FROM quotes_hourly
            WHERE user_id = %s AND ticker = %s
              AND bucket >= NOW() - INTERVAL '%s hours'
            ORDER BY bucket ASC
        """, (self.user_id, ticker, hours))
    else:
        return self.query("""
            SELECT time, price AS close, volume
            FROM market_quotes
            WHERE user_id = %s AND ticker = %s
              AND time >= NOW() - INTERVAL '%s hours'
            ORDER BY time ASC
        """, (self.user_id, ticker, hours))
```

---

## Decision 6 — Idempotency Keys Everywhere (Do This in Sprint 1)

### The problem it solves

Kafka guarantees at-least-once delivery. The Raft log can replay entries after
a leader failover. The ML worker can crash mid-execution and restart. In all three
cases, the same work gets attempted twice. Without idempotency keys, you get:

- Duplicate rows in TimescaleDB (same quote inserted twice)
- Duplicate Qdrant points (same news article embedded twice)
- Duplicate MinIO objects (fine, but wastes API calls)
- Duplicate job run records (incorrect execution history)

The fix is a deterministic idempotency key for every write operation that could
be retried. The key is derived from the input — not a UUID generated at write time.

### The pattern

```python
# Rule: idempotency key = hash(content that must be unique) 
# NOT: idempotency key = uuid() at write time

import hashlib

# For market quotes: content is (user_id, ticker, timestamp truncated to minute)
def quote_idempotency_key(user_id: str, ticker: str, timestamp: datetime) -> str:
    content = f"{user_id}:{ticker}:{timestamp.strftime('%Y-%m-%dT%H:%M')}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]

# For news articles: content is the article URL (globally unique)
def news_idempotency_key(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]

# For job runs: content is (job_id, scheduled_fire_time truncated to minute)
def run_idempotency_key(job_id: str, scheduled_time: datetime) -> str:
    content = f"{job_id}:{scheduled_time.strftime('%Y-%m-%dT%H:%M')}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]

# For ML predictions: content is (user_id, ticker, model, generation_date)
def prediction_idempotency_key(user_id: str, ticker: str, model: str, date: str) -> str:
    content = f"{user_id}:{ticker}:{model}:{date}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]
```

### TimescaleDB upsert pattern

```python
# ml/db/timescale.py
# All inserts use ON CONFLICT DO NOTHING with idempotency key.
# Never use plain INSERT — always upsert.

def upsert_quote(self, ticker: str, price: float, volume: int,
                  timestamp: datetime, source: str = 'yfinance'):
    key = quote_idempotency_key(self.user_id, ticker, timestamp)
    self.execute("""
        INSERT INTO market_quotes
            (time, user_id, ticker, price, volume, source, idempotency_key)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (idempotency_key) DO NOTHING
    """, (timestamp, self.user_id, ticker, price, volume, source, key))

def upsert_indicators(self, ticker: str, indicators: dict, timestamp: datetime):
    key = hashlib.sha256(
        f"{self.user_id}:{ticker}:{timestamp.strftime('%Y-%m-%dT%H:%M')}".encode()
    ).hexdigest()[:16]
    self.execute("""
        INSERT INTO indicators
            (time, user_id, ticker,
             rsi_14, macd, macd_signal, macd_hist,
             bb_upper, bb_middle, bb_lower,
             atr_14, obv, ema_20, sma_50,
             idempotency_key)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (idempotency_key) DO NOTHING
    """, (
        timestamp, self.user_id, ticker,
        indicators.get('rsi'), indicators.get('macd'),
        indicators.get('macd_signal'), indicators.get('macd_hist'),
        indicators.get('bb_upper'), indicators.get('bb_middle'), indicators.get('bb_lower'),
        indicators.get('atr'), indicators.get('obv'),
        indicators.get('ema_20'), indicators.get('sma_50'),
        key
    ))
```

Add `idempotency_key TEXT UNIQUE` to every table in `init.sql` that receives
repeated writes: `market_quotes`, `ohlcv`, `indicators`, `predictions`, `job_runs`.

---

## The Three Scaling Ceilings (And When You Hit Them)

Understanding where the system breaks — before it breaks — is what separates
engineers who build systems from engineers who build demos.

### Ceiling 1 — Raft Cluster Throughput

**What it is:** Single Raft cluster handles ~100 job submissions/second before
quorum latency degrades the scheduler tick loop.

**When you hit it:** ~10,000 concurrent users each submitting 1 job/minute.

**Current protection:** `PartitionKey` field on every job (Decision 2 above).

**How to break through:** Set `sharding_enabled: true`, deploy 2 more Raft clusters,
`ClusterRouter` starts hashing partition keys. No code changes. No migration.

**Interview answer:**
> "The Raft cluster has a quorum latency floor of ~5ms per commit. At personal
> scale that's irrelevant. I designed around it by adding a PartitionKey field
> to every job submission and a ClusterRouter abstraction that currently always
> returns cluster 0. Enabling sharding is a config flag — the routing logic and
> the additional cluster deployments are the only changes needed."

---

### Ceiling 2 — ML Worker CPU Saturation

**What it is:** A single ml-worker container handles ~20 concurrent indicator
computations or ~3 concurrent Prophet forecasts before CPU becomes the bottleneck.

**When you hit it:** ~100 users each running indicator jobs every 5 minutes.

**Current protection:** ML jobs use kafka executor (Decision 3 above). Workers
are stateless consumers in a consumer group.

**How to break through:** `docker-compose.yml` deploy replicas: N. Kafka
distributes `jobs.ml` partitions automatically. No code changes.

**Interview answer:**
> "ML compute is fully decoupled from the scheduler via a Kafka topic. The
> scheduler publishes job envelopes, stateless ML worker containers consume and
> execute. Scaling compute is increasing the replica count in Docker Compose —
> Kafka handles partition distribution automatically. The scheduler never notices."

---

### Ceiling 3 — TimescaleDB Write Throughput

**What it is:** Single TimescaleDB instance handles ~50,000 inserts/second with
compression enabled. Degrades above that due to WAL write pressure.

**When you hit it:** ~5,000 users each sending 1-minute quotes for 20 tickers.
That's 5,000 × 20 = 100,000 inserts/minute — well above the ceiling.

**Current protection:** Compression policy (Decision 5), continuous aggregates,
idempotency keys with ON CONFLICT DO NOTHING (Decision 6).

**How to break through:** TimescaleDB multi-node (distributed hypertables) or
migrate the hot write path to a write-optimized store (Redpanda topic → batch
insert). Both require changes but the `user_id` column (Decision 1) means no
data model changes — only the write path changes.

**Interview answer:**
> "TimescaleDB chunk compression gives 10–20x storage reduction and keeps query
> performance flat as data grows. The ceiling is write throughput — around 50k
> inserts/second for a single node. For personal use that's irrelevant. For open
> source scale I'd move the hot write path to batched inserts from a Kafka consumer
> rather than per-quote inserts, which would push the ceiling to ~500k/second
> before needing distributed hypertables."

---

## Summary — What to Do and When

| Decision | Action | When | Cost if skipped |
|---|---|---|---|
| `user_id` in all tables | Add column + filter in every query | Sprint 1 | Full schema migration across all tables |
| `PartitionKey` in Job struct | Add field, set always, ignore | Sprint 1 | Rewrite job submission API + resharding |
| Kafka executor for ML jobs | Use `kafka` not `shell` for ML | Sprint 2 | Scheduler/ML coupling, false elections |
| Qdrant HNSW parameters | Set at collection creation | Sprint 5 | Recreate collection + re-embed everything |
| TimescaleDB compression + retention | Add to init.sql | Sprint 2 | Manual migration on live data later |
| Idempotency keys everywhere | Hash-based keys on all writes | Sprint 1 | Duplicate data after any restart or failover |

All six decisions have near-zero implementation cost now.
All six decisions have high remediation cost if skipped.

---

## Interview Framing for the Full Scaling Story

When asked "how does this scale?" at any big tech interview, this is the answer:

> "I identified six scalability decisions that are cheap to make upfront and
> expensive to retrofit. Every table has a user_id column for multi-tenancy —
> currently hardcoded to 'default', real UUID when deployed for multiple users,
> zero migration needed. Every job has a PartitionKey field for future Raft
> cluster sharding — the router currently always returns cluster 0, enabling
> sharding is a config flag. ML compute is fully decoupled from the scheduler
> via Kafka — scaling compute is increasing Docker Compose replica count.
> TimescaleDB chunk compression gives 10–20x storage reduction automatically.
> Qdrant collection parameters are set explicitly at creation time because they
> cannot be changed without re-embedding. And every write uses hash-based
> idempotency keys because Kafka and Raft both give at-least-once guarantees —
> without idempotency you get silent data duplication after any restart."

That answer demonstrates you understand distributed systems failure modes,
you planned for scale without over-engineering, and you know the difference
between decisions that compound and decisions that can be deferred.

---

*End of SCALING.md — Version 1.0 — June 2026*
*Read alongside FORWARD.md. Both documents together are the complete implementation contract.*
