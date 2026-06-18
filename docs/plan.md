# SPRINTS.md — Market Intel Platform
## Exact Sprint Plans

> 8 sprints × 2 weeks each. ~5–8 hours/week available.
> Do not start the next sprint until the current acceptance test passes.
> Each sprint ends with something running and demonstrable.

---

## Sprint 1 — Raft Store + Scheduler
**Weeks 1–2 · Now → July 2**

**Goal:** Job submitted via API executes exactly once. Leader failover does not cause double execution.

**Files to create:**
- `internal/raft/` — copy from raft-scheduler repo, do not modify
- `internal/store/types.go` — Job, JobRun, StoreState, Command structs
- `internal/store/store.go` — Submit(), applyLoop(), applyCommand(), RecordRun()
- `internal/store/snapshot.go` — serialize/deserialize StoreState for Raft snapshots
- `internal/store/router.go` — ClusterRouter, always returns 0 now
- `internal/scheduler/scheduler.go` — tickLoop(), fire(), leadership check
- `internal/scheduler/catchup.go` — ReconcileMissedJobs() on startup
- `internal/executor/executor.go` — Executor interface + Dispatcher
- `internal/executor/shell.go` — shell executor with 10min timeout
- `internal/executor/http.go` — HTTP POST executor
- `internal/executor/kafka.go` — publish to Redpanda topic
- `internal/api/handlers.go` — POST /jobs, GET /jobs, GET /jobs/:id, DELETE /jobs/:id, POST /jobs/:id/trigger, GET /cluster
- `cmd/crond/main.go` — wire everything together
- `go.mod` — module: github.com/CJZbeastmode/market-intel
- `docker-compose.yml` — crond-1, crond-2, crond-3 only (no ML services yet)
- `.env.example` — all variables from FORWARD.md section 3
- `config.yaml` — cluster + portfolio sections only

**Scaling hooks (do not skip):**
- `PartitionKey string` on Job struct — set in API handler, always ignored by router
- `user_id TEXT NOT NULL DEFAULT 'default'` in job_runs table
- Idempotency key on every JobRun: `sha256("{job_id}:{scheduled_minute}")[:16]`

**Session breakdown:**
- Session 1: Copy Raft, write types.go, store.go skeleton (Submit + applyLoop)
- Session 2: snapshot.go + router.go + catchup.go
- Session 3: executor interface + shell + http + kafka executors
- Session 4: API handlers + main.go + docker-compose + end-to-end test

**Acceptance test:**
```bash
# Create job, kill leader, restart, verify exactly-once
curl -X POST http://localhost:8080/jobs \
  -d '{"name":"test","cron_expr":"* * * * *","executor":"shell","payload":"echo hi","partition_key":"default"}'
docker stop market-intel-crond-1-1
sleep 15
docker start market-intel-crond-1-1
# job_runs must show 1 row per minute, 0 duplicates
psql -U marketintel -c "
  SELECT idempotency_key, COUNT(*) FROM job_runs
  GROUP BY idempotency_key HAVING COUNT(*) > 1;
"
# Must return 0 rows
```

---

## Sprint 2 — Redpanda + Data Fetching
**Weeks 3–4 · July 2 → July 16**

**Goal:** Live AAPL quote in TimescaleDB within 90 seconds of startup.

**Files to create:**
- `deployment/sql/init.sql` — full schema from FORWARD.md section 5 + idempotency_key UNIQUE on market_quotes, ohlcv, indicators
- `ml/db/timescale.py` — TimescaleClient with upsert_quote(), upsert_ohlcv(), upsert_indicators()
- `ml/db/redis_client.py` — RedisClient with namespaced keys (quote:{user_id}:{ticker})
- `ml/jobs/_base.py` — BaseJob with user_id, logger, db, redis
- `ml/jobs/fetch_quotes.py` — yfinance → Redis + Redpanda market.quotes
- `ml/jobs/fetch_ohlcv.py` — end-of-day OHLCV → TimescaleDB
- `ml/worker.py` — Kafka consumer for jobs.ml topic
- `scripts/init_topics.sh` — create all Redpanda topics with correct partition counts
- `scripts/seed_jobs.sh` — POST default job catalogue from FORWARD.md section 16
- `Dockerfile.crond` — Go binary
- `Dockerfile.ml` — Python 3.11 + requirements

**Update docker-compose.yml** to add: timescaledb, redis, redpanda, ml-worker

**Scaling hooks (do not skip):**
- Compression + retention policies in init.sql (SCALING.md Decision 5)
- Continuous aggregate `quotes_hourly` in init.sql
- All Redpanda topics created with 6 partitions for jobs.ml, 3 for others
- `upsert_quote()` uses `ON CONFLICT (idempotency_key) DO NOTHING`

**Session breakdown:**
- Session 1: init.sql + TimescaleDB schema + docker-compose additions
- Session 2: fetch_quotes.py + redis_client.py + timescale.py upserts
- Session 3: ml/worker.py Kafka consumer + idempotency check
- Session 4: seed_jobs.sh + Dockerfiles + end-to-end test

**Acceptance test:**
```bash
docker compose up -d
sleep 90
psql -U marketintel -c "
  SELECT ticker, price, time FROM market_quotes
  WHERE ticker = 'AAPL' ORDER BY time DESC LIMIT 1;
"
# Must return a row with time within last 2 minutes
```

---

## Sprint 3 — C++ Indicators
**Weeks 5–6 · July 16 → July 30**

**Goal:** RSI for NVDA computed in under 5ms. Results written to TimescaleDB.

**Files to create:**
- `ml/indicators/indicators.cpp` — RSI, EMA, MACD, ATR, Bollinger, OBV, SMA (full code in FORWARD.md section 12)
- `ml/indicators/bindings.cpp` — pybind11 module definition
- `ml/indicators/CMakeLists.txt` — build config with -O3 -march=native
- `ml/jobs/compute_indicators.py` — call C++ extension → upsert_indicators()
- `scripts/build_indicators.sh` — cmake -B build && cmake --build build

**Update Dockerfile.ml** to compile pybind11 extension at image build time.

**Session breakdown:**
- Session 1: CMakeLists.txt + pybind11 install + basic RSI compiling (budget extra time — OS-specific pain likely)
- Session 2: EMA + MACD + ATR in C++
- Session 3: Bollinger + OBV + SMA + bindings.cpp
- Session 4: compute_indicators.py job + upsert + timing test

**Acceptance test:**
```python
import time, indicators
import yfinance as yf
df = yf.download('NVDA', period='3mo', interval='1d')
prices = df['Close'].values.tolist()
start = time.perf_counter()
rsi = indicators.rsi(prices, 14)
elapsed = (time.perf_counter() - start) * 1000
assert elapsed < 5, f"Too slow: {elapsed:.2f}ms"
print(f"PASS — {elapsed:.2f}ms")
```

---

## Sprint 4 — ML Predictions
**Weeks 7–8 · July 30 → Aug 13**

**Goal:** 7-day direction forecast for each ticker saved to TimescaleDB every day after market close.

**Files to create:**
- `ml/models/prophet_model.py` — Prophet baseline, returns direction + confidence + forecast_values
- `ml/models/ensemble.py` — combine prophet output (N-BEATS deferred to post-open-source)
- `ml/jobs/run_predictions.py` — run ensemble for all tickers → upsert predictions table
- `requirements.txt` — add prophet, neuralprophet

**Session breakdown:**
- Session 1: Prophet install + basic model on 90 days OHLCV
- Session 2: Wrap as job, write forecast to predictions table
- Session 3: Add confidence intervals + direction classification logic
- Session 4: Backtest on 30 days held-out data + add to seed_jobs.sh

**Acceptance test:**
```bash
python -m ml.jobs.run_predictions
psql -U marketintel -c "
  SELECT ticker, direction, confidence, time
  FROM predictions
  WHERE time >= NOW() - INTERVAL '2 hours'
  ORDER BY ticker;
"
# Must return one row per ticker with direction in ('up','down','neutral')
```

---

## Sprint 5 — RAG + Qdrant
**Weeks 9–10 · Aug 13 → Aug 27**

**Goal:** Question about AAPL returns relevant news context from Qdrant with score > 0.4.

**Files to create:**
- `ml/rag/embedder.py` — ensure_collection() with explicit HNSW params, embed_and_store(), chunk_text()
- `ml/rag/query.py` — query_qdrant() with ticker/doc_type/date filters
- `ml/jobs/fetch_news.py` — NewsAPI → embed_and_store() → Qdrant
- `ml/jobs/fetch_sec.py` — SEC EDGAR → parse → embed_and_store()

**Update docker-compose.yml** to add: qdrant

**Scaling hooks (do not skip):**
- HNSW params: m=16, ef_construct=100 (SCALING.md Decision 4)
- Payload indexes on ticker, doc_type, user_id, date — created in ensure_collection()
- user_id in every Qdrant point payload

**Session breakdown:**
- Session 1: Qdrant container + ensure_collection() with all indexes
- Session 2: embedder.py — chunk_text() + embed_and_store()
- Session 3: fetch_news.py job + query.py RAG function
- Session 4: fetch_sec.py + end-to-end RAG test

**Acceptance test:**
```python
from ml.rag.query import query_qdrant
results = query_qdrant('AAPL earnings revenue guidance', ticker_filter='AAPL', limit=3)
assert len(results) > 0
assert results[0]['score'] > 0.4
print(f"PASS — score: {results[0]['score']:.3f}")
print(results[0]['text'][:200])
```

---

## Sprint 6 — LangGraph + AI Pipelines
**Weeks 11–12 · Aug 27 → Sep 10**

**Goal:** Daily brief generated and saved to MinIO every morning at 7am.

**Files to create:**
- `ml/ai/openrouter.py` — call_model() with task-based model routing
- `ml/pipelines/daily_brief.py` — 6-node LangGraph pipeline (full code in FORWARD.md section 14)
- `ml/pipelines/earnings.py` — earnings analysis pipeline
- `ml/pipelines/anomalies.py` — anomaly detection pipeline
- `ml/pipelines/server.py` — FastAPI server exposing pipelines as HTTP endpoints (for langgraph executor)

**Update docker-compose.yml** to add: minio, n8n

**Session breakdown:**
- Session 1: openrouter.py + config.yaml AI section + MinIO bucket setup
- Session 2: daily_brief.py nodes 1–3 (fetch_prices, fetch_predictions, fetch_news)
- Session 3: daily_brief.py nodes 4–6 (rag_context, synthesize, store) + pipelines/server.py
- Session 4: earnings.py + anomalies.py + seed generate_daily_brief job

**Acceptance test:**
```bash
python -m ml.pipelines.daily_brief
# Check MinIO for brief
python3 -c "
import boto3, os
from datetime import datetime
s3 = boto3.client('s3', endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin', aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'))
key = f'briefs/default/{datetime.utcnow().strftime(\"%Y-%m-%d\")}/daily_brief.txt'
obj = s3.get_object(Bucket='market-intel', Key=key)
text = obj['Body'].read().decode()
assert len(text) > 200
print('PASS')
print(text[:400])
"
```

---

## Sprint 7 — MCP Server + React Dashboard
**Weeks 13–14 · Sep 10 → Sep 24**

**Goal:** Claude Desktop can create a job via MCP. Dashboard shows live prices updating in real time.

**Files to create:**
- `internal/mcp/server.go` — 8 tools from FORWARD.md section 10
- `internal/api/ws.go` — WebSocket handler, reads from Redis pub/sub on quote:{user_id}:*
- `dashboard/src/views/LiveView.tsx` — portfolio P&L, holdings grid, live chart, AI brief
- `dashboard/src/views/AnalyticsView.tsx` — ML forecasts, indicator charts, earnings calendar
- `dashboard/src/views/ClusterView.tsx` — Raft status, job history, MCP activity log
- `dashboard/src/hooks/useWebSocket.ts` — connection + reconnect logic
- `dashboard/src/components/PriceChart.tsx` — Recharts line chart fed by WebSocket
- `dashboard/src/components/ChatWindow.tsx` — MCP chat interface
- `dashboard/Dockerfile` — nginx serving Vite build

**Session breakdown:**
- Session 1: MCP server — list_jobs, trigger_job, get_cluster_status tools
- Session 2: MCP server — create_job, get_prediction, set_alert, get_news_summary, get_portfolio_risk tools
- Session 3: WebSocket handler + LiveView with real-time price chart
- Session 4: AnalyticsView + ClusterView + dashboard Dockerfile

**Acceptance test:**
```bash
# MCP — Claude Desktop creates a job
curl http://localhost:8080/mcp/tools | jq '[.tools[].name]'
# Must include: create_job, list_jobs, get_prediction, get_news_summary,
#               set_alert, get_portfolio_risk, trigger_job, get_cluster_status

# Dashboard — live prices visible
open http://localhost:3000
# Prices must update without page refresh
```

---

## Sprint 8 — Harden + Deploy
**Weeks 15–16 · Sep 24 → Oct 8**

**Goal:** Kill a Raft node mid-run, restart — no job lost or doubled. System accessible from phone.

**Files to create:**
- `scripts/health_check.sh` — verify all services healthy, exit 1 if not
- `deployment/nginx.conf` — reverse proxy, HTTPS termination for VPS
- `README.md` — 5-minute quickstart, architecture diagram, config reference

**Crash test suite:**
```bash
# scripts/crash_test.sh — run all scenarios, assert no duplicates or missed jobs

echo "=== Test 1: Kill leader during job execution ==="
JOB_ID=$(curl -s -X POST http://localhost:8080/jobs \
  -d '{"name":"crash_test","cron_expr":"* * * * *","executor":"shell","payload":"sleep 5 && echo done"}' \
  | jq -r .job_id)
sleep 62  # wait for first fire
docker stop market-intel-crond-1-1
sleep 15
docker start market-intel-crond-1-1
sleep 120

RUNS=$(psql -U marketintel -tAc "
  SELECT COUNT(*) FROM job_runs
  WHERE job_id = '$JOB_ID'
  AND started_at >= NOW() - INTERVAL '5 minutes';
")
DUPES=$(psql -U marketintel -tAc "
  SELECT COUNT(*) FROM (
    SELECT idempotency_key FROM job_runs
    WHERE job_id = '$JOB_ID'
    GROUP BY idempotency_key HAVING COUNT(*) > 1
  ) t;
")
echo "Runs: $RUNS, Duplicates: $DUPES"
[ "$DUPES" -eq 0 ] && echo "PASS" || echo "FAIL — duplicate executions detected"

echo "=== Test 2: Kill ML worker mid-job ==="
docker stop market-intel-ml-worker-1
sleep 30
docker start market-intel-ml-worker-1
sleep 60
# Verify no duplicate rows in indicators table
DUPES=$(psql -U marketintel -tAc "
  SELECT COUNT(*) FROM (
    SELECT idempotency_key FROM indicators
    GROUP BY idempotency_key HAVING COUNT(*) > 1
  ) t;
")
[ "$DUPES" -eq 0 ] && echo "PASS" || echo "FAIL"

echo "=== Test 3: Full cluster restart ==="
docker compose down
docker compose up -d
sleep 120
# All seeded jobs must be present
JOB_COUNT=$(curl -s http://localhost:8080/jobs | jq '.jobs | length')
echo "Jobs after restart: $JOB_COUNT"
[ "$JOB_COUNT" -ge 14 ] && echo "PASS" || echo "FAIL — jobs lost on restart"
```

**Deploy steps:**
```bash
# VPS (Hetzner CX32 — €8/month) or Raspberry Pi 5
git clone https://github.com/CJZbeastmode/market-intel
cp .env.example .env
# Edit .env: OPENROUTER_API_KEY, TICKERS, DB_PASSWORD, MINIO_SECRET_KEY
docker compose up -d
./scripts/init_topics.sh
./scripts/seed_jobs.sh
# Install Tailscale for anywhere access — no port forwarding needed
curl -fsSL https://tailscale.com/install.sh | sh
tailscale up
```

**Acceptance test:**
```bash
./scripts/crash_test.sh
# All three tests must print PASS
open http://<tailscale-ip>:3000
# Dashboard must load from phone on mobile data (not local network)
```

---

## Sprint Summary

| Sprint | Focus | Weeks | Key Deliverable |
|---|---|---|---|
| 1 | Raft store + scheduler | 1–2 | Exactly-once job execution |
| 2 | Redpanda + data fetching | 3–4 | Live quotes in TimescaleDB |
| 3 | C++ indicators | 5–6 | RSI/MACD in <5ms |
| 4 | ML predictions | 7–8 | 7-day forecasts daily |
| 5 | RAG + Qdrant | 9–10 | Semantic news search |
| 6 | LangGraph + AI | 11–12 | Daily brief in MinIO |
| 7 | MCP + dashboard | 13–14 | Live dashboard + Claude chat |
| 8 | Harden + deploy | 15–16 | Deployed, crash-tested |

**Total: 16 weeks · Now → Oct 8, 2026**

---

*Read alongside FORWARD.md (implementation contracts) and SCALING.md (scalability hooks).*
*All three documents together are the complete build specification.*
