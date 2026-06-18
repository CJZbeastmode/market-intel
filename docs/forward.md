# FORWARD.md — Market Intel Platform
## Complete Implementation Guide for AI-Assisted Development

> This document is the single source of truth for building the market-intel platform.
> It contains architecture decisions, implementation contracts, sprint-by-sprint
> instructions, code patterns, and the exact context an AI tool needs to help
> implement every component correctly. Read this before touching any file.

---

## 0. Project Identity

**Repository:** `market-intel`
**Language split:** Go (scheduler + API + MCP), Python 3.11 (ML + data + AI pipelines), C++ (indicators via pybind11), TypeScript/React (dashboard)
**Infrastructure:** Docker Compose (all environments — laptop, Raspberry Pi, VPS)
**Purpose:** Personal financial intelligence platform. Every data fetch, ML run, and AI pipeline is a scheduled job. Raft consensus guarantees exactly-once execution across crashes and restarts.

**What this is not:** A trading bot. No broker write access. Outputs are informational only.

**Raft implementation source:** Copy from `github.com/CJZbeastmode/raft-scheduler/internal/raft/`. Do not rewrite Raft from scratch. The implementation is complete and stress-tested. Reference it in the README.

---

## 1. Repository Structure (Complete)

```
market-intel/
├── cmd/
│   ├── crond/
│   │   └── main.go                  ← Raft scheduler node entrypoint
│   └── api/
│       └── main.go                  ← REST + WebSocket + MCP server entrypoint
│
├── internal/
│   ├── raft/                        ← COPIED from raft-scheduler repo (do not modify)
│   │   ├── raft.go
│   │   └── persister.go
│   ├── store/
│   │   ├── store.go                 ← Job state machine on top of Raft
│   │   └── snapshot.go              ← Raft snapshot serialization
│   ├── scheduler/
│   │   ├── scheduler.go             ← Cron tick loop + dynamic job creation
│   │   └── catchup.go               ← Reconcile missed jobs on startup
│   ├── executor/
│   │   ├── executor.go              ← Executor interface
│   │   ├── http.go                  ← HTTP/webhook executor
│   │   ├── shell.go                 ← Shell script executor
│   │   ├── kafka.go                 ← Publish to Redpanda topic
│   │   ├── langgraph.go             ← Trigger LangGraph pipeline via HTTP
│   │   └── n8n.go                   ← Trigger n8n workflow via webhook
│   ├── mcp/
│   │   └── server.go                ← MCP protocol server, 8 tools
│   └── api/
│       ├── handlers.go              ← REST endpoints
│       └── ws.go                    ← WebSocket live price feed
│
├── ml/
│   ├── indicators/                  ← C++ source + pybind11 bindings
│   │   ├── indicators.cpp           ← RSI, MACD, Bollinger, ATR, OBV, EMA, SMA
│   │   ├── bindings.cpp             ← pybind11 module definition
│   │   └── CMakeLists.txt           ← Build config
│   ├── models/
│   │   ├── prophet_model.py         ← Prophet baseline forecasting
│   │   ├── nbeats_model.py          ← N-BEATS deep learning model
│   │   └── ensemble.py              ← Combine prophet + nbeats predictions
│   ├── pipelines/
│   │   ├── daily_brief.py           ← LangGraph 6-node daily brief pipeline
│   │   └── earnings.py              ← LangGraph earnings analysis pipeline
│   ├── rag/
│   │   ├── embedder.py              ← Embed documents into Qdrant
│   │   └── query.py                 ← RAG query function
│   ├── jobs/
│   │   ├── fetch_quotes.py          ← yfinance → Redis → Redpanda
│   │   ├── fetch_ohlcv.py           ← End-of-day OHLCV → TimescaleDB
│   │   ├── fetch_news.py            ← NewsAPI → embed → Qdrant
│   │   ├── fetch_sec.py             ← SEC EDGAR → parse → embed → Qdrant
│   │   ├── fetch_earnings_cal.py    ← Earnings calendar → PostgreSQL
│   │   ├── compute_indicators.py    ← C++ indicators → TimescaleDB
│   │   ├── run_predictions.py       ← ML forecast → TimescaleDB
│   │   ├── detect_anomalies.py      ← Stats + Claude → anomaly report
│   │   ├── check_price_alerts.py    ← Redis quote vs threshold → n8n
│   │   └── check_portfolio_risk.py  ← VaR + correlation → Claude → alert
│   └── db/
│       ├── timescale.py             ← TimescaleDB client + upsert helpers
│       └── redis_client.py          ← Redis client + pub/sub helpers
│
├── dashboard/
│   ├── src/
│   │   ├── views/
│   │   │   ├── LiveView.tsx         ← Portfolio P&L + live prices + alerts
│   │   │   ├── AnalyticsView.tsx    ← ML forecasts + indicators + anomalies
│   │   │   └── ClusterView.tsx      ← Raft status + job history + MCP log
│   │   ├── components/
│   │   │   ├── PriceChart.tsx       ← Recharts candlestick/line with WebSocket
│   │   │   ├── ChatWindow.tsx       ← Claude MCP chat interface
│   │   │   └── JobTable.tsx         ← Scheduled jobs + execution history
│   │   ├── hooks/
│   │   │   └── useWebSocket.ts      ← WebSocket connection + reconnect logic
│   │   └── App.tsx
│   ├── package.json
│   └── vite.config.ts
│
├── deployment/
│   ├── docker-compose.yml           ← All services, named volumes
│   ├── docker-compose.dev.yml       ← Dev overrides (hot reload, exposed ports)
│   └── nginx.conf                   ← Reverse proxy for VPS deploy
│
├── scripts/
│   ├── seed_jobs.sh                 ← POST default job catalogue to API on startup
│   ├── build_indicators.sh          ← Compile C++ pybind11 extension
│   └── health_check.sh              ← Verify all services healthy
│
├── config.yaml                      ← Single user-facing config file
├── .env.example                     ← All environment variables documented
├── go.mod
├── go.sum
├── requirements.txt                 ← Python dependencies
├── FORWARD.md                       ← This file
└── README.md                        ← 5-minute quickstart
```

---

## 2. Technology Stack (With Justifications)

Every technology choice is final. Do not suggest alternatives unless a choice blocks
implementation on a specific OS or hardware constraint.

| Layer | Technology | Version | Why |
|---|---|---|---|
| Scheduler | Go | 1.22+ | Same as raft-scheduler. Type-safe, compiled, low overhead |
| Raft consensus | Copied from raft-scheduler | — | Already complete and stress-tested |
| Message bus | Redpanda | latest | Single binary, no ZooKeeper, identical Kafka API |
| Time-series DB | TimescaleDB | latest-pg16 | PostgreSQL extension — familiar SQL, same DB for relational + time-series |
| Vector store | Qdrant | latest | Self-hosted, free, fast, single container |
| Cache | Redis | alpine | Live quote cache + pub/sub for dashboard WebSocket |
| Object store | MinIO | latest | Local S3-compatible — reports, model artifacts, AI outputs |
| ML runtime | Python | 3.11 | LangGraph, PyTorch, Prophet, yfinance all require 3.11+ |
| Indicators | C++ via pybind11 | C++17 | 10–50x faster than NumPy for per-ticker indicator computation |
| LLM routing | OpenRouter | — | One API key, every model, swap per job in config.yaml |
| AI pipelines | LangGraph | latest | Multi-step state machine pipelines with retry and observability |
| Notifications | n8n | latest | Self-hosted, visual workflow editor, supports Slack/email/Discord |
| Dashboard | React 18 + Vite + Recharts | — | WebSocket-native, fast build, Recharts for financial charts |
| Containers | Docker Compose | v2 | Identical setup on laptop, Raspberry Pi, and VPS |

---

## 3. Environment Variables (Complete Reference)

All variables live in `.env`. Never hardcode any of these.

```bash
# Raft cluster
BIND_ADDR=:8001                          # Raft RPC address for this node
PEERS=crond-1:8001,crond-2:8002,crond-3:8003  # All peer addresses
ME=0                                     # 0-indexed position in PEERS
API_ADDR=:8080                           # REST + MCP API address

# Database
DB_HOST=timescaledb
DB_PORT=5432
DB_NAME=marketintel
DB_USER=marketintel
DB_PASSWORD=changeme_in_production

# Redis
REDIS_URL=redis://redis:6379/0

# Redpanda / Kafka
KAFKA_BROKERS=redpanda:9092

# Qdrant
QDRANT_URL=http://qdrant:6333

# MinIO
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=changeme_in_production
MINIO_BUCKET=market-intel

# AI
OPENROUTER_API_KEY=sk-or-...            # Required — get from openrouter.ai
OPENROUTER_BASE_URL=https://openrouter.ai/api/v1

# Data sources
ALPHA_VANTAGE_KEY=                       # Optional — 25 free calls/day
NEWS_API_KEY=                            # Optional — 100 free calls/day

# n8n
N8N_WEBHOOK_BASE_URL=http://n8n:5678/webhook

# Dashboard
VITE_API_URL=http://localhost:8080
VITE_WS_URL=ws://localhost:8080/ws
```

---

## 4. Docker Compose (Complete)

This is the authoritative `docker-compose.yml`. Every sprint adds services to this file.
Named volumes ensure all data survives container restarts.

```yaml
services:

  # ── Raft scheduler nodes ──────────────────────────────────────────────────
  crond-1:
    build:
      context: .
      dockerfile: Dockerfile.crond
    environment:
      BIND_ADDR: ":8001"
      PEERS: "crond-1:8001,crond-2:8002,crond-3:8003"
      ME: "0"
      API_ADDR: ":8080"
      DB_HOST: timescaledb
      REDIS_URL: "redis://redis:6379/0"
      KAFKA_BROKERS: "redpanda:9092"
    volumes:
      - raft-data-1:/data
      - ./config.yaml:/config.yaml:ro
    ports:
      - "8080:8080"   # REST + MCP API (exposed on node 0 only)
    depends_on:
      timescaledb: { condition: service_healthy }
      redis: { condition: service_healthy }
      redpanda: { condition: service_healthy }
    restart: unless-stopped

  crond-2:
    build:
      context: .
      dockerfile: Dockerfile.crond
    environment:
      BIND_ADDR: ":8002"
      PEERS: "crond-1:8001,crond-2:8002,crond-3:8003"
      ME: "1"
      API_ADDR: ":8081"
    volumes:
      - raft-data-2:/data
      - ./config.yaml:/config.yaml:ro
    restart: unless-stopped

  crond-3:
    build:
      context: .
      dockerfile: Dockerfile.crond
    environment:
      BIND_ADDR: ":8003"
      PEERS: "crond-1:8001,crond-2:8002,crond-3:8003"
      ME: "2"
      API_ADDR: ":8082"
    volumes:
      - raft-data-3:/data
      - ./config.yaml:/config.yaml:ro
    restart: unless-stopped

  # ── Storage ───────────────────────────────────────────────────────────────
  timescaledb:
    image: timescale/timescaledb:latest-pg16
    environment:
      POSTGRES_DB: marketintel
      POSTGRES_USER: marketintel
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    volumes:
      - timescale-data:/var/lib/postgresql/data
      - ./deployment/sql/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U marketintel"]
      interval: 5s
      timeout: 5s
      retries: 10
    restart: unless-stopped

  redis:
    image: redis:alpine
    command: redis-server --appendonly yes
    volumes:
      - redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5
    restart: unless-stopped

  qdrant:
    image: qdrant/qdrant:latest
    volumes:
      - qdrant-data:/qdrant/storage
    restart: unless-stopped

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_ACCESS_KEY}
      MINIO_ROOT_PASSWORD: ${MINIO_SECRET_KEY}
    volumes:
      - minio-data:/data
    ports:
      - "9001:9001"   # MinIO console (development only)
    restart: unless-stopped

  # ── Message bus ───────────────────────────────────────────────────────────
  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda
      - start
      - --overprovisioned
      - --smp 1
      - --memory 512M
      - --reserve-memory 0M
      - --node-id 0
      - --kafka-addr PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr PLAINTEXT://redpanda:9092
    volumes:
      - redpanda-data:/var/lib/redpanda/data
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster health | grep -E 'Healthy.+true'"]
      interval: 10s
      timeout: 5s
      retries: 10
    restart: unless-stopped

  # ── ML workers ────────────────────────────────────────────────────────────
  ml-worker:
    build:
      context: .
      dockerfile: Dockerfile.ml
    environment:
      DB_HOST: timescaledb
      REDIS_URL: "redis://redis:6379/0"
      KAFKA_BROKERS: "redpanda:9092"
      QDRANT_URL: "http://qdrant:6333"
      MINIO_ENDPOINT: "minio:9000"
      OPENROUTER_API_KEY: ${OPENROUTER_API_KEY}
      ALPHA_VANTAGE_KEY: ${ALPHA_VANTAGE_KEY}
      NEWS_API_KEY: ${NEWS_API_KEY}
    volumes:
      - ./ml:/app/ml
      - ml-models:/app/models
    depends_on:
      timescaledb: { condition: service_healthy }
      redpanda: { condition: service_healthy }
    restart: unless-stopped

  # ── Notifications ─────────────────────────────────────────────────────────
  n8n:
    image: n8nio/n8n:latest
    environment:
      N8N_BASIC_AUTH_ACTIVE: "true"
      N8N_BASIC_AUTH_USER: admin
      N8N_BASIC_AUTH_PASSWORD: ${DB_PASSWORD}
    volumes:
      - n8n-data:/home/node/.n8n
    ports:
      - "5678:5678"
    restart: unless-stopped

  # ── Dashboard ─────────────────────────────────────────────────────────────
  dashboard:
    build:
      context: ./dashboard
      dockerfile: Dockerfile
    ports:
      - "3000:3000"
    environment:
      VITE_API_URL: http://localhost:8080
      VITE_WS_URL: ws://localhost:8080/ws
    depends_on:
      - crond-1
    restart: unless-stopped

volumes:
  raft-data-1:
  raft-data-2:
  raft-data-3:
  timescale-data:
  redis-data:
  qdrant-data:
  minio-data:
  redpanda-data:
  ml-models:
  n8n-data:
```

---

## 5. TimescaleDB Schema (Complete)

File: `deployment/sql/init.sql` — runs automatically on first container start.

```sql
-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── Market data ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS market_quotes (
    time        TIMESTAMPTZ     NOT NULL,
    user_id     TEXT            NOT NULL DEFAULT 'default',
    ticker      TEXT            NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    volume      BIGINT,
    bid         DOUBLE PRECISION,
    ask         DOUBLE PRECISION,
    source      TEXT            NOT NULL DEFAULT 'yfinance'
);
SELECT create_hypertable('market_quotes', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_quotes_user_ticker_time
    ON market_quotes (user_id, ticker, time DESC);

CREATE TABLE IF NOT EXISTS ohlcv (
    time        TIMESTAMPTZ     NOT NULL,
    user_id     TEXT            NOT NULL DEFAULT 'default',
    ticker      TEXT            NOT NULL,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      BIGINT,
    interval    TEXT            NOT NULL DEFAULT '1d'
);
SELECT create_hypertable('ohlcv', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ohlcv_user_ticker_time
    ON ohlcv (user_id, ticker, time DESC);

CREATE TABLE IF NOT EXISTS indicators (
    time        TIMESTAMPTZ     NOT NULL,
    user_id     TEXT            NOT NULL DEFAULT 'default',
    ticker      TEXT            NOT NULL,
    rsi_14      DOUBLE PRECISION,
    macd        DOUBLE PRECISION,
    macd_signal DOUBLE PRECISION,
    macd_hist   DOUBLE PRECISION,
    bb_upper    DOUBLE PRECISION,
    bb_middle   DOUBLE PRECISION,
    bb_lower    DOUBLE PRECISION,
    atr_14      DOUBLE PRECISION,
    obv         DOUBLE PRECISION,
    ema_20      DOUBLE PRECISION,
    sma_50      DOUBLE PRECISION
);
SELECT create_hypertable('indicators', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_indicators_user_ticker_time
    ON indicators (user_id, ticker, time DESC);

-- ── ML predictions ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS predictions (
    time            TIMESTAMPTZ     NOT NULL,
    user_id         TEXT            NOT NULL DEFAULT 'default',
    ticker          TEXT            NOT NULL,
    model           TEXT            NOT NULL,  -- 'prophet', 'nbeats', 'ensemble'
    horizon_days    INTEGER         NOT NULL,
    direction       TEXT            NOT NULL,  -- 'up', 'down', 'neutral'
    confidence      DOUBLE PRECISION NOT NULL, -- 0.0–1.0
    forecast_values JSONB,                     -- array of {date, value, lower, upper}
    feature_importance JSONB
);
SELECT create_hypertable('predictions', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_predictions_user_ticker_time
    ON predictions (user_id, ticker, time DESC);

-- ── Portfolio ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS portfolio (
    id          SERIAL PRIMARY KEY,
    user_id     TEXT            NOT NULL DEFAULT 'default',
    ticker      TEXT            NOT NULL,
    shares      DOUBLE PRECISION NOT NULL,
    avg_cost    DOUBLE PRECISION NOT NULL,
    added_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, ticker)
);

-- ── Alerts ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS price_alerts (
    id              SERIAL PRIMARY KEY,
    user_id         TEXT            NOT NULL DEFAULT 'default',
    ticker          TEXT            NOT NULL,
    condition       TEXT            NOT NULL,  -- 'above', 'below'
    threshold       DOUBLE PRECISION NOT NULL,
    notification    TEXT            NOT NULL DEFAULT 'n8n',
    triggered_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    active          BOOLEAN         NOT NULL DEFAULT TRUE
);

-- ── Earnings calendar ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS earnings_calendar (
    id              SERIAL PRIMARY KEY,
    user_id         TEXT            NOT NULL DEFAULT 'default',
    ticker          TEXT            NOT NULL,
    report_date     DATE            NOT NULL,
    estimate_eps    DOUBLE PRECISION,
    actual_eps      DOUBLE PRECISION,
    estimate_rev    DOUBLE PRECISION,
    actual_rev      DOUBLE PRECISION,
    beat            BOOLEAN,
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ── Job execution history ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS job_runs (
    id              BIGSERIAL PRIMARY KEY,
    job_id          TEXT            NOT NULL,
    job_name        TEXT            NOT NULL,
    started_at      TIMESTAMPTZ     NOT NULL,
    finished_at     TIMESTAMPTZ,
    status          TEXT            NOT NULL,  -- 'running', 'success', 'failed'
    error_message   TEXT,
    duration_ms     INTEGER,
    idempotency_key TEXT            UNIQUE     -- prevents duplicate execution records
);
CREATE INDEX IF NOT EXISTS idx_job_runs_job_id ON job_runs (job_id, started_at DESC);

-- ── Anomalies ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS anomalies (
    id              BIGSERIAL PRIMARY KEY,
    detected_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    user_id         TEXT            NOT NULL DEFAULT 'default',
    ticker          TEXT,
    anomaly_type    TEXT            NOT NULL,
    description     TEXT            NOT NULL,
    severity        TEXT            NOT NULL,  -- 'low', 'medium', 'high'
    ai_analysis     TEXT
);

-- ── Enable compression on large tables ───────────────────────────────────

SELECT add_compression_policy('market_quotes', INTERVAL '7 days');
SELECT add_compression_policy('ohlcv', INTERVAL '30 days');
SELECT add_compression_policy('indicators', INTERVAL '7 days');
```

---

## 6. Core Go Types (Authoritative Definitions)

All Go code must use these exact types. Do not create alternative structs for the same concept.

```go
// internal/store/types.go

package store

import "time"

// Job is the unit of work stored in the Raft-backed job store.
// PartitionKey is used for future sharding — always set it, always ignore it for routing now.
type Job struct {
    ID           string            `json:"id"`            // UUID v4
    Name         string            `json:"name"`          // human-readable, unique
    CronExpr     string            `json:"cron_expr"`     // standard 5-field cron
    Executor     string            `json:"executor"`      // "shell", "http", "kafka", "langgraph", "n8n"
    Payload      string            `json:"payload"`       // executor-specific (command, URL, topic:msg)
    Enabled      bool              `json:"enabled"`
    CatchupPolicy string           `json:"catchup_policy"` // "skip", "run_once"
    PartitionKey string            `json:"partition_key"`  // ignored now, used for sharding later
    CreatedAt    time.Time         `json:"created_at"`
    UpdatedAt    time.Time         `json:"updated_at"`
    Metadata     map[string]string `json:"metadata"`       // arbitrary key-value for jobs
}

// JobRun is a single execution record.
// IdempotencyKey prevents duplicate execution after leader failover.
type JobRun struct {
    ID              string     `json:"id"`
    JobID           string     `json:"job_id"`
    JobName         string     `json:"job_name"`
    StartedAt       time.Time  `json:"started_at"`
    FinishedAt      *time.Time `json:"finished_at,omitempty"`
    Status          string     `json:"status"`          // "running", "success", "failed"
    ErrorMessage    string     `json:"error_message,omitempty"`
    DurationMs      int64      `json:"duration_ms,omitempty"`
    IdempotencyKey  string     `json:"idempotency_key"` // "{job_id}:{scheduled_time_unix}"
}

// StoreState is the full state serialized into Raft snapshots.
type StoreState struct {
    Jobs    map[string]Job    `json:"jobs"`
    Runs    []JobRun          `json:"runs"`  // last 1000 runs only
}

// Command is a Raft log entry — every mutation goes through Raft.
type Command struct {
    Op   string `json:"op"`   // "create_job", "update_job", "delete_job", "record_run"
    Data []byte `json:"data"` // JSON-encoded Job or JobRun
}
```

---

## 7. Job Store Implementation (Sprint 1 Core)

```go
// internal/store/store.go
// This is the most critical file in the project.
// Every mutation to job state MUST go through raft.Start().
// Never write directly to the in-memory maps without going through Raft.

package store

import (
    "encoding/json"
    "sync"
    "time"

    "github.com/CJZbeastmode/market-intel/internal/raft"
)

type Store struct {
    mu      sync.RWMutex
    rf      *raft.Raft
    me      int
    applyCh chan raft.ApplyMsg

    jobs    map[string]Job
    runs    []JobRun
    waiters map[int]chan struct{} // index → channel closed when committed
}

func NewStore(rf *raft.Raft, me int, applyCh chan raft.ApplyMsg) *Store {
    s := &Store{
        rf:      rf,
        me:      me,
        applyCh: applyCh,
        jobs:    make(map[string]Job),
        runs:    make([]JobRun, 0),
        waiters: make(map[int]chan struct{}),
    }
    go s.applyLoop()
    return s
}

// Submit sends a command through Raft consensus.
// Returns ErrNotLeader if this node is not the current leader.
// Blocks until the command is committed or times out.
func (s *Store) Submit(op string, data interface{}) error {
    payload, err := json.Marshal(data)
    if err != nil {
        return err
    }
    cmd := Command{Op: op, Data: payload}
    cmdBytes, err := json.Marshal(cmd)
    if err != nil {
        return err
    }

    index, _, isLeader := s.rf.Start(cmdBytes)
    if !isLeader {
        return ErrNotLeader
    }

    ch := s.registerWaiter(index)
    select {
    case <-ch:
        return nil
    case <-time.After(5 * time.Second):
        s.removeWaiter(index)
        return ErrTimeout
    }
}

// applyLoop consumes committed entries from Raft and applies them to in-memory state.
// This is the ONLY place that modifies jobs and runs maps.
func (s *Store) applyLoop() {
    for msg := range s.applyCh {
        if msg.CommandValid {
            var cmd Command
            if err := json.Unmarshal(msg.Command.([]byte), &cmd); err != nil {
                continue
            }
            s.mu.Lock()
            s.applyCommand(cmd)
            s.mu.Unlock()
            s.notifyWaiter(msg.CommandIndex)
        } else if msg.SnapshotValid {
            s.applySnapshot(msg.Snapshot)
        }
    }
}

func (s *Store) applyCommand(cmd Command) {
    switch cmd.Op {
    case "create_job", "update_job":
        var job Job
        if err := json.Unmarshal(cmd.Data, &job); err != nil {
            return
        }
        s.jobs[job.ID] = job

    case "delete_job":
        var job Job
        if err := json.Unmarshal(cmd.Data, &job); err != nil {
            return
        }
        delete(s.jobs, job.ID)

    case "record_run":
        var run JobRun
        if err := json.Unmarshal(cmd.Data, &run); err != nil {
            return
        }
        // Idempotency: skip if already recorded
        for _, r := range s.runs {
            if r.IdempotencyKey == run.IdempotencyKey {
                return
            }
        }
        s.runs = append(s.runs, run)
        // Keep only last 1000 runs in memory
        if len(s.runs) > 1000 {
            s.runs = s.runs[len(s.runs)-1000:]
        }
    }
}

// CreateJob creates a new job via Raft consensus.
func (s *Store) CreateJob(job Job) error {
    job.CreatedAt = time.Now()
    job.UpdatedAt = time.Now()
    return s.Submit("create_job", job)
}

// GetJobs returns all jobs. Safe to call from any goroutine.
func (s *Store) GetJobs() []Job {
    s.mu.RLock()
    defer s.mu.RUnlock()
    jobs := make([]Job, 0, len(s.jobs))
    for _, j := range s.jobs {
        jobs = append(jobs, j)
    }
    return jobs
}

// RecordRun writes a job execution record via Raft.
// The idempotency key prevents double-recording after leader failover.
func (s *Store) RecordRun(run JobRun) error {
    return s.Submit("record_run", run)
}
```

---

## 8. Scheduler + Catchup (Sprint 1)

```go
// internal/scheduler/scheduler.go

package scheduler

import (
    "time"
    "github.com/robfig/cron/v3"
    "github.com/CJZbeastmode/market-intel/internal/store"
    "github.com/CJZbeastmode/market-intel/internal/executor"
)

type Scheduler struct {
    store    *store.Store
    executor *executor.Dispatcher
    cron     *cron.Cron
}

func New(s *store.Store, d *executor.Dispatcher) *Scheduler {
    return &Scheduler{
        store:    s,
        executor: d,
        cron:     cron.New(cron.WithSeconds()),
    }
}

// Start begins the cron tick loop.
// Only the Raft leader should be firing jobs — check leadership before each execution.
func (s *Scheduler) Start() {
    go s.tickLoop()
}

func (s *Scheduler) tickLoop() {
    ticker := time.NewTicker(10 * time.Second)
    for range ticker.C {
        _, _, isLeader := s.store.RaftState()
        if !isLeader {
            continue  // followers idle
        }
        jobs := s.store.GetJobs()
        now := time.Now()
        for _, job := range jobs {
            if !job.Enabled {
                continue
            }
            if s.isDue(job, now) {
                go s.fire(job, now)
            }
        }
    }
}

func (s *Scheduler) fire(job store.Job, scheduledTime time.Time) {
    idempotencyKey := job.ID + ":" + scheduledTime.Truncate(time.Minute).Format(time.RFC3339)

    // Check if already fired (duplicate detection after failover)
    if s.store.AlreadyFired(idempotencyKey) {
        return
    }

    run := store.JobRun{
        ID:             newUUID(),
        JobID:          job.ID,
        JobName:        job.Name,
        StartedAt:      time.Now(),
        Status:         "running",
        IdempotencyKey: idempotencyKey,
    }
    s.store.RecordRun(run)

    err := s.executor.Dispatch(job)
    finished := time.Now()
    run.FinishedAt = &finished
    run.DurationMs = finished.Sub(run.StartedAt).Milliseconds()

    if err != nil {
        run.Status = "failed"
        run.ErrorMessage = err.Error()
    } else {
        run.Status = "success"
    }
    s.store.RecordRun(run)
}
```

```go
// internal/scheduler/catchup.go

package scheduler

import (
    "time"
    "github.com/CJZbeastmode/market-intel/internal/store"
)

// ReconcileMissedJobs runs on startup.
// For each job with catchup_policy="run_once", checks if a scheduled run was missed
// while the system was offline and fires it immediately if so.
func ReconcileMissedJobs(s *store.Store, d *executor.Dispatcher) {
    _, _, isLeader := s.RaftState()
    if !isLeader {
        return
    }

    jobs := s.GetJobs()
    now := time.Now()

    for _, job := range jobs {
        if job.CatchupPolicy != "run_once" {
            continue
        }
        lastRun := s.LastRunTime(job.ID)
        nextDue := nextScheduledTime(job.CronExpr, lastRun)

        if nextDue.Before(now) {
            // Job was due while we were offline — fire it now
            go d.Dispatch(job)
        }
    }
}
```

---

## 9. Executor Interface (Sprint 1)

```go
// internal/executor/executor.go

package executor

import "github.com/CJZbeastmode/market-intel/internal/store"

// Executor is the interface all executor types implement.
type Executor interface {
    Execute(job store.Job) error
}

// Dispatcher routes jobs to the correct executor based on job.Executor field.
type Dispatcher struct {
    executors map[string]Executor
}

func NewDispatcher(kafkaBrokers, n8nBaseURL string) *Dispatcher {
    return &Dispatcher{
        executors: map[string]Executor{
            "shell":      &ShellExecutor{},
            "http":       &HTTPExecutor{},
            "kafka":      &KafkaExecutor{Brokers: kafkaBrokers},
            "langgraph":  &LangGraphExecutor{},
            "n8n":        &N8NExecutor{BaseURL: n8nBaseURL},
        },
    }
}

func (d *Dispatcher) Dispatch(job store.Job) error {
    exec, ok := d.executors[job.Executor]
    if !ok {
        return fmt.Errorf("unknown executor type: %s", job.Executor)
    }
    return exec.Execute(job)
}
```

```go
// internal/executor/kafka.go

package executor

import (
    "encoding/json"
    "time"

    "github.com/IBM/sarama"
    "github.com/CJZbeastmode/market-intel/internal/store"
)

type KafkaExecutor struct {
    Brokers string
    producer sarama.SyncProducer
}

// Payload format for kafka executor: "topic:message_json"
// Example: "jobs.ml:{"job_name":"compute_indicators","ticker":"AAPL"}"
func (e *KafkaExecutor) Execute(job store.Job) error {
    // payload format: "topic:json_body"
    parts := strings.SplitN(job.Payload, ":", 2)
    if len(parts) != 2 {
        return fmt.Errorf("kafka executor payload must be 'topic:message'")
    }
    topic, message := parts[0], parts[1]

    envelope := map[string]interface{}{
        "job_id":       job.ID,
        "job_name":     job.Name,
        "triggered_at": time.Now().UTC().Format(time.RFC3339),
        "payload":      message,
    }
    body, _ := json.Marshal(envelope)

    msg := &sarama.ProducerMessage{
        Topic: topic,
        Value: sarama.StringEncoder(body),
        Key:   sarama.StringEncoder(job.ID), // partition by job ID
    }
    _, _, err := e.producer.SendMessage(msg)
    return err
}
```

---

## 10. MCP Server Tools (Sprint 7)

The MCP server exposes the scheduler to Claude Desktop. All 8 tools are defined here.
Implement in `internal/mcp/server.go` using the MCP Go SDK.

```go
// Tool definitions — implement each as a function that calls store/scheduler methods

// create_job
// Input: {"name": string, "cron_expr": string, "executor": string, "payload": string}
// Behavior: Parse natural language cron if needed (call OpenRouter), submit to Raft
// Output: {"job_id": string, "next_run": string}

// list_jobs
// Input: {} or {"filter": "enabled|disabled|all"}
// Output: array of jobs with {id, name, cron_expr, executor, last_run, next_run, status}

// get_prediction
// Input: {"ticker": string, "horizon_days": int}
// Output: {ticker, direction, confidence, forecast_values, model, generated_at}

// get_news_summary
// Input: {"ticker": string, "days": int}
// Behavior: RAG query over Qdrant for ticker news in last N days
// Output: {ticker, summary, sources, retrieved_at}

// set_alert
// Input: {"ticker": string, "condition": "above|below", "threshold": float}
// Behavior: Creates a check_price_alerts job via Raft
// Output: {"alert_id": string, "message": string}

// get_portfolio_risk
// Input: {}
// Behavior: Fetch portfolio from DB, compute VaR + correlation, call Claude for narrative
// Output: {var_95, var_99, correlation_matrix, concentration, ai_narrative}

// trigger_job
// Input: {"job_id": string} or {"job_name": string}
// Behavior: Fire job immediately regardless of schedule, bypass idempotency check
// Output: {"run_id": string, "status": string}

// get_cluster_status
// Input: {}
// Output: {leader_node, term, nodes: [{id, state, last_heartbeat}], recent_runs: [...]}
```

---

## 11. Python ML Worker Pattern

Every Python job follows this exact pattern. Do not deviate from it.

```python
# ml/jobs/_base.py
# All jobs inherit from this. Handles DB connection, logging, error recording.

import os
import sys
import json
import logging
from datetime import datetime
from ml.db.timescale import TimescaleClient
from ml.db.redis_client import RedisClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')

class BaseJob:
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.logger = logging.getLogger(job_name)
        self.db = TimescaleClient()
        self.redis = RedisClient()
        self.user_id = os.getenv('USER_ID', 'default')

    def run(self):
        """Entry point — wraps execute() with error handling and run recording."""
        started_at = datetime.utcnow()
        self.logger.info(f"Starting {self.job_name}")
        try:
            self.execute()
            self.logger.info(f"Completed {self.job_name} in "
                           f"{(datetime.utcnow() - started_at).total_seconds():.2f}s")
        except Exception as e:
            self.logger.error(f"Failed {self.job_name}: {e}", exc_info=True)
            sys.exit(1)

    def execute(self):
        """Override in subclass."""
        raise NotImplementedError
```

```python
# ml/jobs/fetch_quotes.py
# Runs every 1 minute during market hours via shell executor
# Fetches live quotes for all tickers → Redis (cache) → Redpanda (market.quotes topic)

import yfinance as yf
import json
from datetime import datetime
from kafka import KafkaProducer
from ._base import BaseJob

TICKERS = os.getenv('TICKERS', 'AAPL,NVDA,MSFT,TSLA').split(',')

class FetchQuotesJob(BaseJob):
    def __init__(self):
        super().__init__('fetch_quotes')
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv('KAFKA_BROKERS', 'redpanda:9092'),
            value_serializer=lambda v: json.dumps(v).encode()
        )

    def execute(self):
        tickers = yf.Tickers(' '.join(TICKERS))
        for ticker_sym in TICKERS:
            try:
                info = tickers.tickers[ticker_sym].fast_info
                quote = {
                    'ticker': ticker_sym,
                    'price': float(info.last_price),
                    'volume': int(info.three_month_average_volume or 0),
                    'timestamp': datetime.utcnow().isoformat(),
                    'user_id': self.user_id,
                }
                # Write to Redis for live dashboard
                self.redis.client.setex(
                    f"quote:{self.user_id}:{ticker_sym}",
                    120,  # 2 minute TTL
                    json.dumps(quote)
                )
                # Publish to Redpanda for consumers
                self.producer.send('market.quotes', quote)
            except Exception as e:
                self.logger.warning(f"Failed to fetch {ticker_sym}: {e}")

        self.producer.flush()

if __name__ == '__main__':
    FetchQuotesJob().run()
```

---

## 12. C++ Indicators (Sprint 3)

```cpp
// ml/indicators/indicators.cpp
// Compile with: cmake -B build && cmake --build build
// Usage from Python: import indicators; rsi = indicators.rsi(prices, 14)

#include <vector>
#include <stdexcept>
#include <cmath>

// RSI — Relative Strength Index
// prices: closing prices, period: lookback (default 14)
std::vector<double> rsi(const std::vector<double>& prices, int period = 14) {
    if (prices.size() < static_cast<size_t>(period + 1)) {
        throw std::invalid_argument("Not enough data for RSI calculation");
    }
    std::vector<double> result(prices.size(), std::numeric_limits<double>::quiet_NaN());
    double avg_gain = 0.0, avg_loss = 0.0;

    for (int i = 1; i <= period; ++i) {
        double change = prices[i] - prices[i-1];
        if (change > 0) avg_gain += change;
        else avg_loss -= change;
    }
    avg_gain /= period;
    avg_loss /= period;

    if (avg_loss == 0.0) {
        result[period] = 100.0;
    } else {
        result[period] = 100.0 - (100.0 / (1.0 + avg_gain / avg_loss));
    }

    for (size_t i = period + 1; i < prices.size(); ++i) {
        double change = prices[i] - prices[i-1];
        double gain = (change > 0) ? change : 0.0;
        double loss = (change < 0) ? -change : 0.0;
        avg_gain = (avg_gain * (period - 1) + gain) / period;
        avg_loss = (avg_loss * (period - 1) + loss) / period;
        result[i] = (avg_loss == 0.0) ? 100.0 : 100.0 - (100.0 / (1.0 + avg_gain / avg_loss));
    }
    return result;
}

// EMA — Exponential Moving Average (used internally by MACD)
std::vector<double> ema(const std::vector<double>& prices, int period) {
    std::vector<double> result(prices.size(), std::numeric_limits<double>::quiet_NaN());
    double multiplier = 2.0 / (period + 1);
    double sum = 0.0;
    for (int i = 0; i < period; ++i) sum += prices[i];
    result[period - 1] = sum / period;
    for (size_t i = period; i < prices.size(); ++i) {
        result[i] = (prices[i] - result[i-1]) * multiplier + result[i-1];
    }
    return result;
}

// MACD — returns {macd_line, signal_line, histogram}
struct MACDResult {
    std::vector<double> macd;
    std::vector<double> signal;
    std::vector<double> histogram;
};

MACDResult macd(const std::vector<double>& prices,
                int fast = 12, int slow = 26, int signal_period = 9) {
    auto ema_fast = ema(prices, fast);
    auto ema_slow = ema(prices, slow);
    MACDResult result;
    result.macd.resize(prices.size(), std::numeric_limits<double>::quiet_NaN());

    for (size_t i = slow - 1; i < prices.size(); ++i) {
        result.macd[i] = ema_fast[i] - ema_slow[i];
    }
    result.signal = ema(result.macd, signal_period);
    result.histogram.resize(prices.size(), std::numeric_limits<double>::quiet_NaN());
    for (size_t i = 0; i < prices.size(); ++i) {
        if (!std::isnan(result.macd[i]) && !std::isnan(result.signal[i])) {
            result.histogram[i] = result.macd[i] - result.signal[i];
        }
    }
    return result;
}

// ATR — Average True Range
std::vector<double> atr(const std::vector<double>& highs,
                         const std::vector<double>& lows,
                         const std::vector<double>& closes,
                         int period = 14) {
    size_t n = closes.size();
    std::vector<double> tr(n, 0.0);
    tr[0] = highs[0] - lows[0];
    for (size_t i = 1; i < n; ++i) {
        double hl = highs[i] - lows[i];
        double hc = std::abs(highs[i] - closes[i-1]);
        double lc = std::abs(lows[i] - closes[i-1]);
        tr[i] = std::max({hl, hc, lc});
    }
    std::vector<double> result(n, std::numeric_limits<double>::quiet_NaN());
    double sum = 0.0;
    for (int i = 0; i < period; ++i) sum += tr[i];
    result[period - 1] = sum / period;
    for (size_t i = period; i < n; ++i) {
        result[i] = (result[i-1] * (period - 1) + tr[i]) / period;
    }
    return result;
}
```

```cpp
// ml/indicators/bindings.cpp
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "indicators.cpp"

namespace py = pybind11;

PYBIND11_MODULE(indicators, m) {
    m.doc() = "C++ financial indicator calculations via pybind11";

    m.def("rsi", &rsi, "Relative Strength Index",
          py::arg("prices"), py::arg("period") = 14);

    m.def("ema", &ema, "Exponential Moving Average",
          py::arg("prices"), py::arg("period"));

    m.def("atr", &atr, "Average True Range",
          py::arg("highs"), py::arg("lows"), py::arg("closes"), py::arg("period") = 14);

    py::class_<MACDResult>(m, "MACDResult")
        .def_readonly("macd", &MACDResult::macd)
        .def_readonly("signal", &MACDResult::signal)
        .def_readonly("histogram", &MACDResult::histogram);

    m.def("macd", &macd, "MACD indicator",
          py::arg("prices"), py::arg("fast") = 12,
          py::arg("slow") = 26, py::arg("signal_period") = 9);
}
```

---

## 13. OpenRouter Model Routing Config

```yaml
# config.yaml (AI section — full reference)
ai:
  provider: openrouter
  api_key: ${OPENROUTER_API_KEY}
  base_url: https://openrouter.ai/api/v1

  models:
    # Free tier — use for high-frequency, low-complexity tasks
    news_summarization:   meta-llama/llama-3.1-8b-instruct:free
    rag_queries:          meta-llama/llama-3.1-8b-instruct:free
    anomaly_classification: meta-llama/llama-3.1-8b-instruct:free

    # Cheap tier (~$0.001/call) — daily recurring analysis
    daily_brief:          deepseek/deepseek-chat
    indicator_explanation: deepseek/deepseek-chat
    earnings_summary:     deepseek/deepseek-chat

    # Premium (~$0.01/call) — complex reasoning, once per day max
    portfolio_recommendation: anthropic/claude-sonnet-4-6
    earnings_deep_dive:       anthropic/claude-sonnet-4-6
    weekly_roundup:           anthropic/claude-sonnet-4-6
    portfolio_risk:           anthropic/claude-sonnet-4-6
```

```python
# ml/ai/openrouter.py
# Single client used by all pipelines and jobs

import os
import httpx
from typing import Optional

OPENROUTER_BASE = os.getenv('OPENROUTER_BASE_URL', 'https://openrouter.ai/api/v1')

def call_model(
    task: str,          # key from config.yaml ai.models
    messages: list,
    max_tokens: int = 1000,
    temperature: float = 0.3,
    system: Optional[str] = None
) -> str:
    """Route a task to the appropriate model via OpenRouter."""
    import yaml
    with open('/config.yaml') as f:
        config = yaml.safe_load(f)
    model = config['ai']['models'].get(task, 'meta-llama/llama-3.1-8b-instruct:free')

    payload = {
        "model": model,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": messages,
    }
    if system:
        payload["messages"] = [{"role": "system", "content": system}] + messages

    response = httpx.post(
        f"{OPENROUTER_BASE}/chat/completions",
        headers={
            "Authorization": f"Bearer {os.getenv('OPENROUTER_API_KEY')}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60.0
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]
```

---

## 14. LangGraph Daily Brief Pipeline (Sprint 6)

```python
# ml/pipelines/daily_brief.py

from langgraph.graph import StateGraph, END
from typing import TypedDict, List, Optional
from datetime import datetime, timedelta
from ml.ai.openrouter import call_model
from ml.rag.query import query_qdrant
from ml.db.timescale import TimescaleClient
import boto3, os, json

class BriefState(TypedDict):
    date: str
    tickers: List[str]
    prices: dict           # {ticker: {price, change_pct, volume}}
    predictions: dict      # {ticker: {direction, confidence}}
    news_chunks: List[str] # retrieved from Qdrant
    rag_context: str       # synthesized from news_chunks
    brief_text: str        # final output
    stored: bool

def fetch_portfolio_prices(state: BriefState) -> BriefState:
    db = TimescaleClient()
    for ticker in state['tickers']:
        row = db.query_one("""
            SELECT price, time
            FROM market_quotes
            WHERE ticker = %s AND user_id = 'default'
            ORDER BY time DESC LIMIT 1
        """, (ticker,))
        prev = db.query_one("""
            SELECT close FROM ohlcv
            WHERE ticker = %s AND user_id = 'default'
            AND time >= NOW() - INTERVAL '2 days'
            ORDER BY time DESC LIMIT 1 OFFSET 1
        """, (ticker,))
        if row:
            change_pct = ((row['price'] - prev['close']) / prev['close'] * 100) if prev else 0
            state['prices'][ticker] = {
                'price': row['price'],
                'change_pct': round(change_pct, 2),
                'time': row['time'].isoformat()
            }
    return state

def fetch_ml_predictions(state: BriefState) -> BriefState:
    db = TimescaleClient()
    for ticker in state['tickers']:
        row = db.query_one("""
            SELECT direction, confidence, forecast_values
            FROM predictions
            WHERE ticker = %s AND user_id = 'default'
            AND model = 'ensemble'
            ORDER BY time DESC LIMIT 1
        """, (ticker,))
        if row:
            state['predictions'][ticker] = {
                'direction': row['direction'],
                'confidence': row['confidence'],
            }
    return state

def fetch_recent_news(state: BriefState) -> BriefState:
    # Retrieve recent news chunks from Qdrant for all portfolio tickers
    chunks = []
    for ticker in state['tickers']:
        results = query_qdrant(
            query=f"latest news about {ticker} stock price performance earnings",
            ticker_filter=ticker,
            limit=3,
            days_back=2
        )
        chunks.extend([r['text'] for r in results])
    state['news_chunks'] = chunks[:15]  # cap at 15 chunks
    return state

def rag_query_context(state: BriefState) -> BriefState:
    if not state['news_chunks']:
        state['rag_context'] = "No recent news available."
        return state

    # Summarize news chunks with free model
    chunks_text = "\n\n---\n\n".join(state['news_chunks'])
    state['rag_context'] = call_model(
        task='news_summarization',
        messages=[{
            "role": "user",
            "content": f"Summarize these financial news excerpts in 3-4 sentences, "
                      f"focusing on price-moving information:\n\n{chunks_text}"
        }],
        max_tokens=300
    )
    return state

def synthesize_brief(state: BriefState) -> BriefState:
    prices_text = json.dumps(state['prices'], indent=2)
    predictions_text = json.dumps(state['predictions'], indent=2)

    state['brief_text'] = call_model(
        task='daily_brief',
        system="You are a concise financial analyst writing a personal daily brief. "
               "Be direct, use numbers, no fluff.",
        messages=[{
            "role": "user",
            "content": f"""Write a daily brief for {state['date']}.

Portfolio prices:
{prices_text}

ML predictions (7-day):
{predictions_text}

Recent news context:
{state['rag_context']}

Format: 3-4 paragraphs. Lead with overall portfolio sentiment. 
Then notable movers. Then forward-looking based on predictions.
End with one watchlist item."""
        }],
        max_tokens=600
    )
    return state

def store_brief(state: BriefState) -> BriefState:
    # Store in MinIO
    s3 = boto3.client('s3',
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY')
    )
    key = f"briefs/{state['date']}/daily_brief.txt"
    s3.put_object(
        Bucket=os.getenv('MINIO_BUCKET', 'market-intel'),
        Key=key,
        Body=state['brief_text'].encode(),
        ContentType='text/plain'
    )

    # Also push to Redis so dashboard can read it instantly
    from ml.db.redis_client import RedisClient
    RedisClient().client.set('latest_brief', state['brief_text'], ex=86400)
    state['stored'] = True
    return state

# Build the graph
def build_daily_brief_pipeline() -> StateGraph:
    graph = StateGraph(BriefState)

    graph.add_node('fetch_prices', fetch_portfolio_prices)
    graph.add_node('fetch_predictions', fetch_ml_predictions)
    graph.add_node('fetch_news', fetch_recent_news)
    graph.add_node('rag_context', rag_query_context)
    graph.add_node('synthesize', synthesize_brief)
    graph.add_node('store', store_brief)

    graph.set_entry_point('fetch_prices')
    graph.add_edge('fetch_prices', 'fetch_predictions')
    graph.add_edge('fetch_prices', 'fetch_news')
    graph.add_edge('fetch_predictions', 'synthesize')
    graph.add_edge('fetch_news', 'rag_context')
    graph.add_edge('rag_context', 'synthesize')
    graph.add_edge('synthesize', 'store')
    graph.add_edge('store', END)

    return graph.compile()

if __name__ == '__main__':
    import os
    pipeline = build_daily_brief_pipeline()
    result = pipeline.invoke({
        'date': datetime.utcnow().strftime('%Y-%m-%d'),
        'tickers': os.getenv('TICKERS', 'AAPL,NVDA,MSFT').split(','),
        'prices': {},
        'predictions': {},
        'news_chunks': [],
        'rag_context': '',
        'brief_text': '',
        'stored': False,
    })
    print(result['brief_text'])
```

---

## 15. Qdrant RAG Setup (Sprint 5)

```python
# ml/rag/embedder.py

from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import (
    VectorParams, Distance, HnswConfigDiff,
    PointStruct, Filter, FieldCondition, MatchValue
)
import os, hashlib, textwrap
from datetime import datetime

COLLECTION = 'financial_docs'
MODEL_NAME = 'all-MiniLM-L6-v2'  # 384 dimensions, free, fast, no API key
CHUNK_SIZE = 512
CHUNK_OVERLAP = 64

model = SentenceTransformer(MODEL_NAME)
client = QdrantClient(url=os.getenv('QDRANT_URL', 'http://qdrant:6333'))

def ensure_collection():
    """Create collection with explicit HNSW parameters if it doesn't exist."""
    collections = [c.name for c in client.get_collections().collections]
    if COLLECTION not in collections:
        client.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE),
            hnsw_config=HnswConfigDiff(
                m=16,             # connections per node — 16 is good for recall/speed
                ef_construct=100, # build quality — higher = better recall, slower index
            )
        )
        # Payload indexes for filtered search
        client.create_payload_index(COLLECTION, 'ticker', 'keyword')
        client.create_payload_index(COLLECTION, 'doc_type', 'keyword')
        client.create_payload_index(COLLECTION, 'date', 'datetime')

def chunk_text(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    words = text.split()
    chunks, i = [], 0
    while i < len(words):
        chunk = ' '.join(words[i:i+size])
        chunks.append(chunk)
        i += size - overlap
    return chunks

def embed_and_store(doc_id: str, text: str, metadata: dict):
    """
    Embed a document and store in Qdrant.
    metadata must contain: ticker, doc_type ('news'|'filing'|'transcript'), date (ISO string)
    """
    ensure_collection()
    chunks = chunk_text(text)
    embeddings = model.encode(chunks, batch_size=32, show_progress_bar=False)

    points = []
    for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
        point_id = hashlib.md5(f"{doc_id}_{i}".encode()).hexdigest()[:8]
        # Convert hex to int for Qdrant ID
        point_id_int = int(point_id, 16)
        points.append(PointStruct(
            id=point_id_int,
            vector=emb.tolist(),
            payload={
                'text': chunk,
                'doc_id': doc_id,
                'chunk_index': i,
                'ticker': metadata.get('ticker', ''),
                'doc_type': metadata.get('doc_type', 'unknown'),
                'date': metadata.get('date', datetime.utcnow().isoformat()),
                'source': metadata.get('source', ''),
            }
        ))

    client.upsert(collection_name=COLLECTION, points=points)
```

```python
# ml/rag/query.py

from qdrant_client.models import Filter, FieldCondition, MatchValue, Range
from .embedder import model, client, COLLECTION
from datetime import datetime, timedelta

def query_qdrant(
    query: str,
    ticker_filter: str = None,
    doc_type_filter: str = None,
    days_back: int = 7,
    limit: int = 5
) -> list[dict]:
    """
    Semantic search over financial documents.
    Returns list of {text, ticker, doc_type, date, score}
    """
    query_vector = model.encode(query).tolist()

    # Build filters
    must_conditions = []
    if ticker_filter:
        must_conditions.append(
            FieldCondition(key='ticker', match=MatchValue(value=ticker_filter))
        )
    if doc_type_filter:
        must_conditions.append(
            FieldCondition(key='doc_type', match=MatchValue(value=doc_type_filter))
        )
    if days_back:
        cutoff = (datetime.utcnow() - timedelta(days=days_back)).isoformat()
        must_conditions.append(
            FieldCondition(key='date', range=Range(gte=cutoff))
        )

    query_filter = Filter(must=must_conditions) if must_conditions else None

    results = client.search(
        collection_name=COLLECTION,
        query_vector=query_vector,
        query_filter=query_filter,
        limit=limit,
        with_payload=True,
        score_threshold=0.4  # ignore irrelevant results
    )

    return [
        {
            'text': r.payload['text'],
            'ticker': r.payload.get('ticker', ''),
            'doc_type': r.payload.get('doc_type', ''),
            'date': r.payload.get('date', ''),
            'score': r.score,
        }
        for r in results
    ]
```

---

## 16. Default Job Catalogue

These jobs are seeded via `scripts/seed_jobs.sh` on first startup.
Every job is submitted via `POST /jobs` to the API.

```bash
#!/bin/bash
# scripts/seed_jobs.sh
API="http://localhost:8080"

post_job() {
    curl -sf -X POST "$API/jobs" \
        -H "Content-Type: application/json" \
        -d "$1" > /dev/null && echo "Created: $(echo $1 | jq -r .name)"
}

# Data ingestion
post_job '{"name":"fetch_live_quotes","cron_expr":"*/1 * * * 1-5",
           "executor":"kafka","payload":"jobs.ml:{\"job\":\"fetch_quotes\"}",
           "catchup_policy":"skip","enabled":true}'

post_job '{"name":"fetch_daily_ohlcv","cron_expr":"0 18 * * 1-5",
           "executor":"shell","payload":"python -m ml.jobs.fetch_ohlcv",
           "catchup_policy":"run_once","enabled":true}'

post_job '{"name":"fetch_news","cron_expr":"*/30 * * * *",
           "executor":"shell","payload":"python -m ml.jobs.fetch_news",
           "catchup_policy":"skip","enabled":true}'

post_job '{"name":"fetch_sec_filings","cron_expr":"0 20 * * 1-5",
           "executor":"shell","payload":"python -m ml.jobs.fetch_sec",
           "catchup_policy":"run_once","enabled":true}'

post_job '{"name":"fetch_earnings_calendar","cron_expr":"0 7 * * 1",
           "executor":"shell","payload":"python -m ml.jobs.fetch_earnings_cal",
           "catchup_policy":"run_once","enabled":true}'

# ML and analysis
post_job '{"name":"compute_indicators","cron_expr":"*/5 * * * 1-5",
           "executor":"kafka","payload":"jobs.ml:{\"job\":\"compute_indicators\"}",
           "catchup_policy":"skip","enabled":true}'

post_job '{"name":"run_predictions","cron_expr":"0 19 * * 1-5",
           "executor":"shell","payload":"python -m ml.jobs.run_predictions",
           "catchup_policy":"run_once","enabled":true}'

post_job '{"name":"detect_anomalies","cron_expr":"0 */6 * * *",
           "executor":"langgraph","payload":"http://ml-worker:8000/pipelines/anomalies",
           "catchup_policy":"skip","enabled":true}'

post_job '{"name":"generate_daily_brief","cron_expr":"0 7 * * 1-5",
           "executor":"langgraph","payload":"http://ml-worker:8000/pipelines/daily_brief",
           "catchup_policy":"run_once","enabled":true}'

post_job '{"name":"generate_weekly_roundup","cron_expr":"0 8 * * 1",
           "executor":"langgraph","payload":"http://ml-worker:8000/pipelines/weekly_roundup",
           "catchup_policy":"run_once","enabled":true}'

post_job '{"name":"retrain_models","cron_expr":"0 2 * * 0",
           "executor":"shell","payload":"python -m ml.jobs.retrain",
           "catchup_policy":"skip","enabled":true}'

# Alerts and notifications
post_job '{"name":"check_price_alerts","cron_expr":"*/1 * * * 1-5",
           "executor":"shell","payload":"python -m ml.jobs.check_price_alerts",
           "catchup_policy":"skip","enabled":true}'

post_job '{"name":"send_morning_digest","cron_expr":"0 7 * * 1-5",
           "executor":"n8n","payload":"morning_digest",
           "catchup_policy":"run_once","enabled":true}'

post_job '{"name":"check_portfolio_risk","cron_expr":"0 16 * * 1-5",
           "executor":"langgraph","payload":"http://ml-worker:8000/pipelines/portfolio_risk",
           "catchup_policy":"skip","enabled":true}'

echo "All default jobs seeded."
```

---

## 17. Sprint Acceptance Tests (Do Not Proceed Without Passing)

```bash
# Sprint 1 — Raft store + scheduler
# Create a job, kill the leader, restart, verify exactly-once execution
curl -X POST http://localhost:8080/jobs \
  -d '{"name":"test_job","cron_expr":"* * * * *","executor":"shell","payload":"echo hello"}'
docker stop market-intel-crond-1-1  # kill leader
sleep 15                             # wait for new election
docker start market-intel-crond-1-1
# Check job_runs table — exactly 1 entry per minute, no duplicates
psql -U marketintel -c "SELECT job_name, idempotency_key, status FROM job_runs ORDER BY started_at;"

# Sprint 2 — Kafka + data fetching
# AAPL quote must appear in TimescaleDB within 90 seconds
sleep 90
psql -U marketintel -c "SELECT ticker, price, time FROM market_quotes WHERE ticker='AAPL' ORDER BY time DESC LIMIT 1;"

# Sprint 3 — C++ indicators
# RSI for NVDA must compute in under 5ms
python3 -c "
import time, indicators
import yfinance as yf
df = yf.download('NVDA', period='3mo', interval='1d')
prices = df['Close'].values.tolist()
start = time.perf_counter()
rsi = indicators.rsi(prices, 14)
elapsed = (time.perf_counter() - start) * 1000
print(f'RSI computed in {elapsed:.2f}ms')
assert elapsed < 5, f'Too slow: {elapsed:.2f}ms'
print('PASS')
"

# Sprint 4 — ML predictions
psql -U marketintel -c "
  SELECT ticker, direction, confidence, time
  FROM predictions
  WHERE time >= NOW() - INTERVAL '25 hours'
  ORDER BY time DESC;
"

# Sprint 5 — RAG
python3 -c "
from ml.rag.query import query_qdrant
results = query_qdrant('AAPL earnings revenue guidance', ticker_filter='AAPL', limit=3)
assert len(results) > 0, 'No results returned'
assert results[0]['score'] > 0.4, f'Low relevance: {results[0][\"score\"]}'
print(f'PASS — top result score: {results[0][\"score\"]:.3f}')
print(results[0]['text'][:200])
"

# Sprint 6 — LangGraph daily brief
python3 -m ml.pipelines.daily_brief
# Check MinIO for generated brief
python3 -c "
import boto3, os
s3 = boto3.client('s3', endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin', aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'))
from datetime import datetime
key = f'briefs/{datetime.utcnow().strftime(\"%Y-%m-%d\")}/daily_brief.txt'
obj = s3.get_object(Bucket='market-intel', Key=key)
print(obj['Body'].read().decode()[:500])
print('PASS')
"

# Sprint 7 — MCP + dashboard
# Claude Desktop must be able to list jobs
curl http://localhost:8080/mcp/tools | jq '.tools[].name'
# Dashboard must show live prices
open http://localhost:3000

# Sprint 8 — Crash test
docker stop market-intel-crond-1-1
sleep 30
docker start market-intel-crond-1-1
sleep 60
psql -U marketintel -c "
  SELECT job_name, COUNT(*) as run_count,
         COUNT(DISTINCT idempotency_key) as unique_runs
  FROM job_runs
  WHERE started_at >= NOW() - INTERVAL '10 minutes'
  GROUP BY job_name
  HAVING COUNT(*) != COUNT(DISTINCT idempotency_key);
"
# Query must return 0 rows — no duplicate executions
```

---

## 18. Key Design Decisions — Do Not Revisit

These decisions are final. If an AI tool suggests alternatives, decline and continue
with what is specified here.

1. **Raft consensus for job store** — not Postgres advisory locks, not Redis SETNX, not ZooKeeper. Raft. Copied from raft-scheduler.

2. **Redpanda not full Kafka** — single binary, no ZooKeeper dependency, identical consumer API. Swap to Kafka later without code changes.

3. **TimescaleDB not InfluxDB** — standard SQL, same DB instance for relational + time-series data, `user_id` column in every table from day one.

4. **Qdrant not Pinecone/Weaviate** — self-hosted, free, single container. Explicit HNSW parameters set at collection creation time.

5. **OpenRouter not direct API calls** — one key, model swapping per job in config.yaml, no code changes to switch models.

6. **pybind11 for C++ indicators** — not Cython, not ctypes. pybind11 gives clean Python bindings with minimal boilerplate.

7. **`user_id DEFAULT 'default'` in all tables** — multi-tenancy ready from day one, zero cost at personal scale.

8. **`PartitionKey` in Job struct** — ignored now, enables sharding across multiple Raft clusters without schema migration.

9. **Kafka executor for ML jobs** — ML workers consume from `jobs.ml` topic, decoupling compute load from scheduler nodes.

10. **Manual Kafka offset commit** — commit only after successful DB write. Guarantees at-least-once processing with idempotency keys preventing double-writes.

---

## 19. What Links Where (For README and CV)

```markdown
# In market-intel README.md

## Raft consensus layer
The distributed scheduler is backed by a Raft consensus implementation
written from scratch. See [raft-scheduler](https://github.com/CJZbeastmode/raft-scheduler)
for the standalone implementation, stress tests, and architecture notes.

## Related projects
- [raft-scheduler](https://github.com/CJZbeastmode/raft-scheduler) — standalone Raft-backed cron scheduler (Go)
- [home-widgets-system](https://github.com/CJZbeastmode/home-widgets-system) — microservices widget aggregation platform (Python/React/Swift)
```

---

*End of FORWARD.md — Version 1.0 — June 2026*
*Update this document when any architectural decision changes. Keep it as the AI context file.*
