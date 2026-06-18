CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS market_quotes (
    time            TIMESTAMPTZ      NOT NULL,
    user_id         TEXT             NOT NULL DEFAULT 'default',
    ticker          TEXT             NOT NULL,
    price           DOUBLE PRECISION NOT NULL,
    volume          BIGINT,
    bid             DOUBLE PRECISION,
    ask             DOUBLE PRECISION,
    source          TEXT             NOT NULL DEFAULT 'yfinance',
    idempotency_key TEXT             NOT NULL
);
SELECT create_hypertable('market_quotes', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_quotes_user_ticker_time
    ON market_quotes (user_id, ticker, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_quotes_time_idempotency
    ON market_quotes (time, idempotency_key);

CREATE TABLE IF NOT EXISTS ohlcv (
    time            TIMESTAMPTZ      NOT NULL,
    user_id         TEXT             NOT NULL DEFAULT 'default',
    ticker          TEXT             NOT NULL,
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    volume          BIGINT,
    interval        TEXT             NOT NULL DEFAULT '1d',
    idempotency_key TEXT             NOT NULL
);
SELECT create_hypertable('ohlcv', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ohlcv_user_ticker_time
    ON ohlcv (user_id, ticker, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ohlcv_time_idempotency
    ON ohlcv (time, idempotency_key);

CREATE TABLE IF NOT EXISTS indicators (
    time            TIMESTAMPTZ      NOT NULL,
    user_id         TEXT             NOT NULL DEFAULT 'default',
    ticker          TEXT             NOT NULL,
    rsi_14          DOUBLE PRECISION,
    macd            DOUBLE PRECISION,
    macd_signal     DOUBLE PRECISION,
    macd_hist       DOUBLE PRECISION,
    bb_upper        DOUBLE PRECISION,
    bb_middle       DOUBLE PRECISION,
    bb_lower        DOUBLE PRECISION,
    atr_14          DOUBLE PRECISION,
    obv             DOUBLE PRECISION,
    ema_20          DOUBLE PRECISION,
    sma_50          DOUBLE PRECISION,
    idempotency_key TEXT             NOT NULL
);
SELECT create_hypertable('indicators', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_indicators_user_ticker_time
    ON indicators (user_id, ticker, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_indicators_time_idempotency
    ON indicators (time, idempotency_key);

CREATE TABLE IF NOT EXISTS predictions (
    time               TIMESTAMPTZ      NOT NULL,
    user_id            TEXT             NOT NULL DEFAULT 'default',
    ticker             TEXT             NOT NULL,
    model              TEXT             NOT NULL,
    horizon_days       INTEGER          NOT NULL,
    direction          TEXT             NOT NULL,
    confidence         DOUBLE PRECISION NOT NULL,
    forecast_values    JSONB,
    feature_importance JSONB,
    idempotency_key    TEXT             NOT NULL
);
SELECT create_hypertable('predictions', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_predictions_user_ticker_time
    ON predictions (user_id, ticker, time DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_predictions_time_idempotency
    ON predictions (time, idempotency_key);

CREATE TABLE IF NOT EXISTS portfolio (
    id       SERIAL PRIMARY KEY,
    user_id  TEXT             NOT NULL DEFAULT 'default',
    ticker   TEXT             NOT NULL,
    shares   DOUBLE PRECISION NOT NULL,
    avg_cost DOUBLE PRECISION NOT NULL,
    added_at TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, ticker)
);

CREATE TABLE IF NOT EXISTS price_alerts (
    id           SERIAL PRIMARY KEY,
    user_id      TEXT             NOT NULL DEFAULT 'default',
    ticker       TEXT             NOT NULL,
    condition    TEXT             NOT NULL,
    threshold    DOUBLE PRECISION NOT NULL,
    notification TEXT             NOT NULL DEFAULT 'n8n',
    triggered_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    active       BOOLEAN          NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS earnings_calendar (
    id           SERIAL PRIMARY KEY,
    user_id      TEXT        NOT NULL DEFAULT 'default',
    ticker       TEXT        NOT NULL,
    report_date  DATE        NOT NULL,
    estimate_eps DOUBLE PRECISION,
    actual_eps   DOUBLE PRECISION,
    estimate_rev DOUBLE PRECISION,
    actual_rev   DOUBLE PRECISION,
    beat         BOOLEAN,
    fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS calendar_events (
    id              BIGSERIAL   PRIMARY KEY,
    user_id         TEXT        NOT NULL DEFAULT 'default',
    event_type      TEXT        NOT NULL,
    ticker          TEXT,
    event_date      DATE        NOT NULL,
    event_time      TEXT,
    impact          TEXT        NOT NULL,
    detail          JSONB,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    idempotency_key TEXT        UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_calendar_user_date
    ON calendar_events (user_id, event_date ASC);
CREATE INDEX IF NOT EXISTS idx_calendar_user_type
    ON calendar_events (user_id, event_type, event_date ASC);

CREATE TABLE IF NOT EXISTS job_runs (
    id              BIGSERIAL PRIMARY KEY,
    job_id          TEXT        NOT NULL,
    job_name        TEXT        NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    status          TEXT        NOT NULL,
    error_message   TEXT,
    duration_ms     INTEGER,
    idempotency_key TEXT        UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_job_runs_job_id
    ON job_runs (job_id, started_at DESC);

CREATE TABLE IF NOT EXISTS anomalies (
    id           BIGSERIAL PRIMARY KEY,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    user_id      TEXT        NOT NULL DEFAULT 'default',
    ticker       TEXT,
    anomaly_type TEXT        NOT NULL,
    description  TEXT        NOT NULL,
    severity     TEXT        NOT NULL,
    ai_analysis  TEXT
);

ALTER TABLE market_quotes SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'user_id,ticker'
);
ALTER TABLE ohlcv SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'user_id,ticker'
);
ALTER TABLE indicators SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'user_id,ticker'
);

SELECT add_compression_policy('market_quotes', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_compression_policy('ohlcv', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_compression_policy('indicators', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('market_quotes', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('indicators', INTERVAL '90 days', if_not_exists => TRUE);

CREATE MATERIALIZED VIEW IF NOT EXISTS quotes_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    user_id,
    ticker,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(volume) AS volume
FROM market_quotes
GROUP BY bucket, user_id, ticker
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'quotes_hourly',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);
CREATE INDEX IF NOT EXISTS idx_quotes_hourly_user_ticker_bucket
    ON quotes_hourly (user_id, ticker, bucket DESC);
