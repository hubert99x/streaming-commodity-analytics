# Technical Documentation — Commodity Price Streaming System

> Critical analysis. Last updated: 2026-03-19 (terminology and consistency revision).

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Data Flow](#2-data-flow)
3. [Component Descriptions](#3-component-descriptions)
4. [Database Design](#4-database-design)
5. [dbt Transformation Layer](#5-dbt-transformation-layer)
6. [Monitoring & Alerting](#6-monitoring--alerting)
7. [Security Model](#7-security-model)
8. [Deployment Instructions](#8-deployment-instructions)
9. [CI/CD Pipeline](#9-cicd-pipeline)
10. [Critical Weaknesses & Recommendations](#10-critical-weaknesses--recommendations)

---

## 1. Architecture Overview

The system is a single-machine, Docker Compose-based streaming analytics pipeline for commodity prices (Gold/XAU, Bitcoin/BTC, EUR/USD). It ingests data from the Twelve Data REST API, streams through Apache Kafka, processes with Spark Structured Streaming into PostgreSQL, transforms with dbt into analytical models, and visualizes in Grafana.

### High-Level Architecture

```
┌──────────────┐     ┌─────────────┐     ┌──────────────────┐     ┌──────────────┐
│  Twelve Data │     │    Kafka    │     │  Spark Structured│     │  PostgreSQL  │
│   REST API   │────▶│  (KRaft)   │────▶│    Streaming     │────▶│   16.13      │
│              │     │ 3 partitions│     │                  │     │              │
└──────────────┘     └─────────────┘     └──────────────────┘     └──────┬───────┘
       ▲                                                                  │
       │                                                           ┌──────┴───────┐
  ┌────┴─────┐                                                     │     dbt      │
  │ Producer │                                                     │  (6m cycle)  │
  │ (Python) │                                                     └──────┬───────┘
  └──────────┘                                                            │
                                                                   ┌──────┴───────┐
                                                                   │   Grafana    │
                                                                   │  Dashboards  │
                                                                   │  + Alerts    │
                                                                   └──────────────┘
```

### Service Inventory (14 total)

| Service | Image | Profile | Resource Limits |
|---------|-------|---------|-----------------|
| postgres | postgres:16.13 | always | 512MB / 1.0 CPU |
| kafka | confluentinc/cp-kafka:7.6.1 (KRaft) | always | 1GB / 1.0 CPU |
| spark-stream | apache/spark:3.5.1 | always | 1GB / 1.5 CPU |
| spark (debug) | apache/spark:3.5.1 | always | 1GB / 1.0 CPU |
| producer | python:3.12-slim (custom) | always | 128MB / 0.25 CPU |
| dbt-scheduler | python:3.12-slim (custom, dbt-postgres 1.9.0) | always | 256MB / 0.5 CPU |
| dbt (manual) | python:3.12-slim (custom, dbt-postgres 1.9.0) | always | 256MB / 0.5 CPU |
| grafana | grafana/grafana:11.0.0 | always | 256MB / 0.5 CPU |
| alert-receiver | python:3.12-slim (custom) | always | 128MB / 0.25 CPU |
| pgadmin | dpage/pgadmin4:8.14 | dev | 256MB / 0.5 CPU |
| kafka-ui | provectuslabs/kafka-ui:v0.7.2 | dev | 256MB / 0.5 CPU |
| kafka-lag | python:3.12-slim (custom) | ops | 128MB / 0.25 CPU |
| retention | postgres:16.13 (custom) | ops | 128MB / 0.25 CPU |
| backup-cron | postgres:16.13 | ops | 256MB / 0.5 CPU |

### Network Topology

Three isolated Docker bridge networks enforce segmentation:

```
┌─── database ──────────────────────────────────────────────────────┐
│  postgres, spark-stream, spark, producer, dbt, dbt-scheduler,    │
│  grafana, pgadmin, kafka-lag, alert-receiver, retention,         │
│  backup-cron                                                     │
└──────────────────────────────────────────────────────────────────-┘

┌─── messaging ────────────────────────────────────────────────────┐
│  kafka, spark-stream, spark, producer, kafka-ui, kafka-lag       │
└──────────────────────────────────────────────────────────────────-┘

┌─── frontend ─────────────────────────────────────────────────────┐
│  grafana, pgadmin, kafka-ui, alert-receiver                      │
└──────────────────────────────────────────────────────────────────-┘
```

All external ports bind to `127.0.0.1` (Grafana:3000, pgAdmin:5050, Kafka UI:8080).

**Weakness:** The network segmentation is logical, not cryptographic. All inter-service traffic is unencrypted plaintext. A compromised container on the `database` network can sniff Postgres credentials in transit.

---

## 2. Data Flow

### End-to-End Pipeline

```
                   6-min polling cycle
                         │
                         ▼
 ┌─────────────────────────────────────────────┐
 │              PRODUCER (Python)               │
 │  1. Check FX weekend gate (XAU, EUR skip     │
 │     Fri 22:00 → Sun 21:59 UTC)              │
 │  2. GET /price from Twelve Data API          │
 │  3. Generate deterministic event_id (UUID5)  │
 │  4. Publish JSON to Kafka topic              │
 │  5. Log API metrics to monitoring.api_calls  │
 └──────────────────┬──────────────────────────-┘
                    │ commodity_prices topic
                    │ (3 partitions, key=commodity)
                    ▼
 ┌─────────────────────────────────────────────┐
 │       SPARK STRUCTURED STREAMING             │
 │  Trigger: every 300s                         │
 │  1. Read microbatch from Kafka               │
 │  2. Parse JSON, validate schema              │
 │  3. Route bad records → DLQ (monitoring)     │
 │  4. Write to staging table (ingest schema)   │
 │  5. Acquire advisory lock                    │
 │  6. MERGE into raw_prices ON CONFLICT SKIP   │
 │  7. Release lock, truncate staging           │
 └──────────────────┬──────────────────────────-┘
                    │
                    ▼
 ┌─────────────────────────────────────────────┐
 │           POSTGRESQL (public.raw_prices)     │
 │  Idempotent sink: ON CONFLICT DO NOTHING     │
 └──────────────────┬──────────────────────────-┘
                    │
                    ▼
 ┌─────────────────────────────────────────────┐
 │            dbt (every 6 minutes)             │
 │  analytics.stg_raw_prices        (view)      │
 │  analytics.mart_latest_prices    (view)      │
 │  analytics.mart_minute_last_price (incr.)    │
 │  analytics.mart_price_events      (incr.)    │
 │  analytics.mart_price_volatility_1h (incr.)  │
 └──────────────────┬──────────────────────────-┘
                    │
                    ▼
 ┌─────────────────────────────────────────────┐
 │        GRAFANA (3 dashboards, 11 alerts)      │
 │  market_overview, market_analysis,           │
 │  pipeline_&_data_quality                     │
 │                                              │
 │  Alert webhook → alert-receiver → Postgres   │
 └─────────────────────────────────────────────-┘
```

### Effectively-Once Delivery Analysis

The pipeline achieves **effectively-once** semantics through layered idempotency:

| Layer | Mechanism | Guarantee |
|-------|-----------|-----------|
| Producer → Kafka | `enable.idempotence=True`, `acks=all`, deterministic UUID5 event_id | At-least-once (Kafka deduplicates producer retries) |
| Kafka → Spark | Checkpoint-based offset tracking (not consumer groups) | At-least-once (replays from checkpoint on crash) |
| Spark → Postgres | `ON CONFLICT (event_id) DO NOTHING` | At-most-once per event_id (duplicates silently dropped) |
| **Combined** | | **Effectively-once at Postgres level** (idempotent, not transactional exactly-once) |

**Observability:** The Spark streaming job tracks ON CONFLICT discards via `executeUpdate()` row counts. Each batch logs `conflict_skipped=N`, making it possible to distinguish healthy idempotent replays from data quality problems causing unexpected ID collisions.

---

## 3. Component Descriptions

### 3.1 Producer (`producer/producer.py`)

**Role:** Polls Twelve Data REST API every 6 minutes, publishes price events to Kafka.

**Key behaviors:**
- **Deterministic event IDs:** UUID5 (DNS namespace + `commodity:ISO_timestamp`). Same commodity+timestamp always produces same ID, preventing semantic duplicates across retries.
- **FX weekend gating:** XAU/USD and EUR/USD are not fetched Fri 22:00 – Sun 21:59 UTC. BTC runs 24/7.
- **Three-tier backoff:** Rate limit (429) → fixed backoff; server error (5xx) → exponential backoff with multiplier (1→2→4→...32); non-server `RuntimeError` → interval-length backoff; generic exceptions → exponential backoff (same multiplier as 5xx). All clamped to 15–3600s range (max backoff = 10× polling interval).
- **Kafka producer config:** `enable.idempotence=True`, `acks=all`, `retries=10`, `linger.ms=0`.
- **Pre-publish price bounds validation:** Checks prices against commodity-specific bounds (XAU: 500–15000, BTC: 100–1M, EUR: 0.5–2.0) before publishing to Kafka. Out-of-bounds prices are logged and skipped, preventing pipeline contamination at the source.
- **API metrics:** Each API call is logged to `monitoring.api_calls` (status code, latency, error message) via a lazy Postgres connection.
- **Graceful shutdown:** SIGINT/SIGTERM handlers flush the Kafka producer buffer before exit.

**Weaknesses:**
- **No circuit breaker.** Exponential backoff can reach 10+ minutes. There is no alert mechanism if the producer enters prolonged backoff — the system just goes quiet.
- **Global mutable `_pg_conn`.** Safe in the current single-threaded design, but will silently corrupt if the producer is ever made concurrent.

### 3.2 Spark Structured Streaming (`spark/stream_to_postgres.py`)

**Role:** Consumes from Kafka, validates records, routes bad records to DLQ, writes good records to Postgres.

**Key behaviors:**
- **Trigger:** 300-second processing intervals. `maxOffsetsPerTrigger=5000` limits backpressure.
- **Offset management:** Checkpoint directory (not Kafka consumer groups). Each Spark instance maintains its own offset state.
- **Validation pipeline:** Multi-level checks per record (logic extracted to `spark/validation.py` for testability — 27 unit tests). Price bounds are duplicated across Spark (`spark/validation.py`) and producer (`producer.py:69-73`) — the producer maintains its own copy for early rejection before Kafka publish. These are kept in sync manually, not imported from a shared module. Validation checks per record:
  - Null field detection (MISSING_FIELD errors)
  - Price positivity check
  - Schema version check (must be `1`)
  - Commodity-specific price bounds (XAU: 500–15000, BTC: 100–1M, EUR: 0.5–2.0)
- **Staging table pattern:** Good records → `ingest.raw_prices_staging` (truncate-append-merge). PostgreSQL advisory lock (key 1) serializes concurrent merges.
- **DLQ:** Bad records → `ingest.dlq_staging` → merged into `monitoring.dead_letter_events` with its own advisory lock (key 2) and unique constraint to prevent duplicates on batch replay.
- **JDBC timeouts:** `connectTimeout` and `socketTimeout` (configurable via `JDBC_CONNECT_TIMEOUT` and `JDBC_SOCKET_TIMEOUT` env vars, defaults 10s/30s) prevent indefinite hangs on Postgres connection issues.
- **Health check:** Docker liveness probe checks both that `spark-submit` process is alive and that checkpoint directory was modified within the last 10 minutes, detecting both crashes and stalled processing.
- **Deduplication:** Handled entirely by PostgreSQL `ON CONFLICT (event_id) DO NOTHING` — no in-batch `dropDuplicates` needed. **Conflict-skipped rows are counted and logged** (`conflict_skipped=N`) for observability, distinguishing healthy idempotent replays from data quality issues.
- **DLQ write failure tracking:** If DLQ staging fails, the lost record count is logged with a structured `DLQ_WRITE_FAILURE` tag and `lost_records=N` for grep-based alerting.
- **Advisory lock retry:** `pg_advisory_unlock` retries up to 3 times before giving up, preventing deadlocks from transient connection issues.

**Weaknesses:**
- **Price bounds are hardcoded.** Changing thresholds (e.g., if gold exceeds $15,000) requires a code change and container rebuild. No external configuration mechanism exists.
- **Concurrent instance race.** If two Spark instances start during a restart (old shutting down, new starting up), both read the same checkpoint and process overlapping offset ranges. Advisory locks protect the staging merge but not the consumption itself. Idempotency at the Postgres layer saves correctness, but batch metrics become misleading (double-counted).
- **`failOnDataLoss=false`** means Kafka offset gaps (e.g., from topic retention) are silently ignored. No alert fires when Spark skips over lost offsets.

### 3.3 dbt Scheduler (`ops/dbt-scheduler/`)

**Role:** Runs `dbt build` every 6 minutes and `dbt test` (with result logging) every 30 minutes.

**Key behaviors:**
- Python-based scheduler with `threading.Lock()` to prevent overlapping runs.
- Health heartbeat: writes timestamp to `/tmp/dbt_scheduler_alive`, checked by Docker health probe with 10-minute tolerance.
- `dbt test` results parsed from `target/run_results.json` via `jq` and inserted into `monitoring.dbt_test_runs`.
- 300-second timeout on subprocess execution.
- **Automated retention:** Deletes records older than 90 days from all data and monitoring tables every 24 hours (configurable via `RETENTION_INTERVAL_SEC`). Runs in parallel with the standalone retention daemon (ops profile); both are idempotent.
- **Non-root execution:** Runs as UID 1000 (`USER 1000` in Dockerfile). Container hardened with `cap_drop: ALL`.

**Weaknesses:**
- **If dbt build consistently exceeds 6 minutes, runs are silently skipped** (lock contention). There is no alert for "dbt build took too long" — only the file-marker health check would eventually fail after 10 minutes of no heartbeat.

**Note:** Build duration is now tracked — each run logs `duration_ms=N` to stdout for operational visibility and trend analysis.

### 3.4 Alert Receiver (`ops/alert-receiver/app.py`)

**Role:** Flask webhook listener that receives Grafana alert notifications and stores them in Postgres.

**Key behaviors:**
- `POST /grafana` accepts Grafana alert JSON.
- Flexible field extraction handles Grafana API version differences.
- **Mandatory `X-Webhook-Token` header validation.** The service refuses to start without `ALERT_WEBHOOK_TOKEN` set, unless explicitly opted out with `ALERT_WEBHOOK_AUTH_DISABLED=true`.
- Stores raw JSONB payload alongside parsed fields for debugging.

### 3.5 Kafka Lag Monitor (`ops/kafka-lag/kafka_lag.py`)

**Role:** Measures Spark consumer lag by comparing Kafka high-water marks against the latest offsets written to Postgres.

**Key design:** Because Spark uses checkpoint-based offsets (not consumer groups), this service cannot use standard Kafka consumer-group lag tools. Instead, it queries `MAX(kafka_offset)` per partition from `public.raw_prices` and compares against Kafka's `get_watermark_offsets()`.

**Weakness:** Lag is measured against successfully-written Postgres rows, not against Spark's internal checkpoint. If Spark consumes a batch but fails during the staging merge, the lag monitor reports it as unconsumed — which is technically correct from a data perspective but may overstate the actual problem.

### 3.6 Backup & Retention

**Backup (`backup-cron`):** `pg_dump -F c` every 2 hours, keeps last 360 dumps (~30 days). Logs to `monitoring.backup_log`.

**Retention (`retention`):** Long-running daemon (restart: unless-stopped, every 24h). Deletes records older than 90 days from all 7 tables (`raw_prices`, `dead_letter_events`, `alert_events`, `api_calls`, `kafka_lag`, `dbt_test_runs`, `backup_log`) via shared `retention.sql`. Additionally runs `VACUUM (ANALYZE)` on `raw_prices` on Sundays only. Interval configurable via `RETENTION_INTERVAL_SEC`.

**Weaknesses:**
- **Backups are unencrypted.** Stored as plain `pg_dump` files on the host filesystem. No encryption at rest.
- **No restore testing.** No automated verification that backups can be successfully restored.

**Note:** Retention runs in two places: the dbt-scheduler (built-in, every 24h) and the standalone `retention` daemon (ops profile, every 24h). Both execute the same `retention.sql` and are idempotent. The standalone daemon additionally runs `VACUUM (ANALYZE)` on `raw_prices` on Sundays. All tables — `raw_prices`, `dead_letter_events`, `alert_events`, `api_calls`, `kafka_lag`, `dbt_test_runs`, `backup_log` — are pruned at 90-day TTL. The `dbt_runner` role has been granted DELETE on the relevant tables.

---

## 4. Database Design

### Schema Layout

```
commodities (database)
├── public
│   └── raw_prices              ← Spark sink (idempotent upsert)
├── ingest
│   ├── raw_prices_staging      ← Spark per-batch staging (persistent, truncated between batches)
│   └── dlq_staging             ← DLQ staging
├── analytics (dbt)
│   ├── stg_raw_prices          ← View (type casting, timezone)
│   ├── mart_latest_prices      ← View (latest price per commodity)
│   ├── mart_minute_last_price  ← Incremental (1-min OHLC buckets)
│   ├── mart_price_events       ← Incremental (significant moves)
│   └── mart_price_volatility_1h ← Incremental (hourly volatility)
└── monitoring
    ├── api_calls               ← Producer API metrics
    ├── dead_letter_events      ← Malformed Kafka records
    ├── kafka_lag               ← Consumer lag time series
    ├── alert_events            ← Grafana alert history
    ├── dbt_test_runs           ← dbt test results
    ├── backup_log              ← Backup status
    ├── pipeline_metrics (view) ← Single-row health summary
    ├── api_metrics_18m (view)  ← 18m rolling API stats
    └── kafka_lag_latest (view) ← Latest lag per group
```

### PostgreSQL Tuning

PostgreSQL is configured with performance tuning via `docker-compose.yml` command flags:

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `shared_buffers` | 128MB | Increased from default 32MB for better caching |
| `effective_cache_size` | 384MB | Helps query planner choose index scans |
| `random_page_cost` | 1.1 | Tuned for SSD/container storage (default 4.0) |
| `checkpoint_completion_target` | 0.9 | Spreads checkpoint writes over longer period |

### `raw_prices` Table Schema

```sql
event_id    TEXT PRIMARY KEY          -- UUID5 (deterministic)
commodity   TEXT NOT NULL             -- gold, bitcoin, eurusd
symbol      TEXT NOT NULL             -- XAU/USD, BTC/USD, EUR/USD
price       DOUBLE PRECISION NOT NULL -- Validated: positive + within bounds
currency    TEXT NOT NULL             -- Always "USD"
event_ts    TIMESTAMP NOT NULL        -- Source timestamp (UTC)
source      TEXT                      -- "twelvedata_rest"
ingest_ts   TIMESTAMP                 -- Spark processing timestamp
kafka_partition INTEGER               -- Audit trail
kafka_offset    BIGINT                -- Audit trail
```

### Indexes

| Index | Columns | Purpose |
|-------|---------|---------|
| `idx_raw_prices_commodity_event_ts` | (commodity, event_ts DESC, event_id DESC) | Latest price per commodity queries |
| `idx_raw_prices_event_ts` | (event_ts DESC) | Time-range filters, staleness checks |
| `idx_raw_prices_event_ts_brin` | (event_ts) BRIN | Efficient time-range scans with minimal storage overhead |
| `idx_raw_prices_partition_offset` | (kafka_partition, kafka_offset DESC) | Kafka lag monitor |
| `uq_dlq_event` (unique) | (stream_instance_id, batch_id, kafka_partition, kafka_offset) | DLQ batch replay dedup |

### Role-Based Access Control (5 roles)

| Role | Schemas | Permissions |
|------|---------|-------------|
| `spark_writer` | public, ingest, monitoring | INSERT raw_prices, CREATE staging tables, INSERT DLQ |
| `dbt_runner` | public, analytics, monitoring | SELECT raw_prices, CREATE analytics models, DELETE monitoring (retention) |
| `grafana_read` | analytics, monitoring | SELECT on all analytics tables (auto-granted on new dbt objects) + SELECT on all monitoring tables and views |
| `producer_writer` | monitoring | INSERT api_calls |
| `backup_user` | all | SELECT on all schemas + INSERT backup_log (used by pg_dump and backup logging) |

**Notable:** `DEFAULT PRIVILEGES FOR USER dbt_runner` auto-grants SELECT to `grafana_read` on any new table dbt creates. This is a well-designed pattern that prevents missing grants when models are added.

**Weakness:** The `backup_user` role passes the password via `PGPASSWORD` environment variable, which is visible in `docker inspect` and `/proc` on the host. A `.pgpass` file with restricted permissions would be more secure.

---

## 5. dbt Transformation Layer

**Configuration:** Profile `commodity_dbt`, target `dev`, schema `analytics`, **4 threads** for parallel model execution.

### Model Dependency Graph

```
public.raw_prices
       │
       ▼
stg_raw_prices (VIEW)
       │
       ├──▶ mart_latest_prices      (VIEW)
       ├──▶ mart_minute_last_price  (INCREMENTAL, 30m lookback)
       ├──▶ mart_price_events       (INCREMENTAL, 2h lookback)
       └──▶ mart_price_volatility_1h (INCREMENTAL, 2h lookback)
```

### Model Details

#### `stg_raw_prices` (View)
Pass-through with explicit type casts and timezone normalization (`timestamptz` → naive UTC). Preserves Kafka partition/offset for lineage tracing. Defined via `{{ source('public', 'raw_prices') }}` with **source freshness SLA** (warn after 10 minutes, error after 20 minutes).

#### `mart_latest_prices` (View)
One row per commodity. Uses PostgreSQL `DISTINCT ON` with a 24-hour optimization window — scans last 24 hours first, falls back to full scan only for commodities missing from that window. Materialized as a view so each query reads the latest data directly from the staging layer.

#### `mart_minute_last_price` (Incremental, 30-min lookback)
1-minute OHLC-style buckets. Picks the **last** price per minute via `array_agg(price ORDER BY event_ts DESC)[1]`. Includes event count (`n`), min/max price. Post-hook creates a `(minute_bucket DESC)` index.

#### `mart_price_events` (Incremental, 2-hour lookback)
Detects significant price movements using `LAG()` window function with **commodity-specific thresholds**:

| Commodity | EXTREME_MOVE | LARGE_MOVE | MEDIUM_MOVE |
|-----------|-------------|-----------|------------|
| BTC/USD | ≥ 1.5% | ≥ 0.7% | ≥ 0.3% |
| XAU/USD | ≥ 0.6% | ≥ 0.3% | ≥ 0.15% |
| EUR/USD | ≥ 0.25% | ≥ 0.12% | ≥ 0.06% |

Excludes observations after a >30-minute gap (prevents false extreme events from FX weekend close/reopen). Only outputs non-NORMAL events. Uses deterministic `LAG()` ordering (`ORDER BY event_ts, event_id`) to prevent non-deterministic results on timestamp ties.

#### `mart_price_volatility_1h` (Incremental, 2-hour lookback)
Hourly volatility: stddev, range, range_pct (`(max-min)/avg * 100`). Excludes current incomplete hour.

### Data Quality Tests (64)

- **Staging:** not_null and unique on event_id; accepted_values on commodity and currency; freshness bounds (`event_ts` within -24h to +1min of `ingest_ts`).
- **Marts:** unique combination checks on composite keys; price sanity (> 0, min ≤ max); event_type accepted values.
- **Custom SQL test:** `test_price_jump.sql` — detects unrealistic minute-to-minute jumps per commodity (EUR >5%, XAU >10%, BTC >30%).

**Weaknesses:**
- **Incremental lookback edge case.** If `dbt build` is skipped for >30 minutes (scheduler blocked or container restarting), `mart_minute_last_price`'s 30-minute lookback window may miss late-arriving data from before the gap.
- **`mart_latest_prices` is a view.** Each Grafana query executes the underlying SQL against `stg_raw_prices`. The 24-hour optimization window limits scan scope, but the fallback full scan may slow down if `raw_prices` grows significantly.
- **Hardcoded thresholds.** Price event thresholds and bounds are embedded in SQL. Changing them requires a dbt rebuild and potential reprocessing of the incremental lookback window.

---

## 6. Monitoring & Alerting

### Alert Rules (11 total, 30-second evaluation)

| Alert | Condition | Severity | Fires After |
|-------|-----------|----------|-------------|
| Stale Ingest (>13m) | `time_since_last_ingest_seconds > 780` | CRITICAL | 2 min |
| BTC Events Low (15m) | `btc_events_last_15m < 2` | WARNING | 2 min |
| API Errors ≥1 (18m) | `errors_18m >= 1` | WARNING | 2 min |
| API Errors ≥3 (18m) | `errors_18m >= 3` | CRITICAL | 2 min |
| DLQ Events (15m) | DLQ count > 0 in 15m window | WARNING | 2 min |
| dbt Test Failures (35m) | `status = 'FAIL'` in `dbt_test_runs` (35m window) | WARNING | 2 min |
| Kafka Lag >50 | `total_lag > 50` | WARNING | 2 min |
| Kafka Lag >500 | `total_lag > 500` | CRITICAL | 2 min |
| Kafka Partition Lag >30 | `max_partition_lag > 30` | WARNING | 2 min |
| No Backup in 25h | No `status='OK'` row in `backup_log` for >25h | WARNING | 2 min |
| Stale mart_latest_prices | `last_timestamp` older than 15 minutes | WARNING | 2 min |

**Routing:** Critical and warning alerts → `postgres-webhook` contact point → alert-receiver → `monitoring.alert_events`.

**`noDataState`:** Most alerts fire on no-data (no data = something is broken), except DLQ (no data = no bad records = OK).

### Monitoring Views (materialized as SQL views)

- **`pipeline_metrics`:** Single-row summary with `time_since_last_ingest_seconds`, `events_last_6m`, `btc_events_last_15m`.
- **`api_metrics_18m`:** Rolling 18-minute API success rate.
- **`kafka_lag_latest`:** Latest lag per consumer group via `DISTINCT ON`.

### Monitoring Gaps

1. ~~**No dbt health alert.**~~ **Addressed.** dbt source freshness SLA (warn 10m, error 20m) detects stale `raw_prices` input. Alert rule `dbt_test_failures_35m` fires when any dbt test run reports `status=FAIL` within a 35-minute window, covering both test assertion failures and execution errors.
2. ~~**No dbt build duration tracking.**~~ **Addressed.** Build duration (`duration_ms`) is now logged to stdout for each run (OK, FAILED, TIMEOUT).
3. ~~**No per-partition Kafka lag.**~~ **Addressed.** Alert rule `kafka_partition_lag_warn_gt_30` fires when `max_partition_lag > 30` for 2 minutes, detecting stuck partitions masked by healthy total lag.
4. **No Spark streaming metrics.** Microbatch duration, watermark lag, and task counts are not exposed. *(Partially mitigated: batch log now includes `inserted`, `conflict_skipped`, `dlq_write_failed`, `ms` per batch.)*
5. **No Postgres table size monitoring.** No alert for `raw_prices` approaching disk capacity.
6. **No alert for alert-receiver downtime.** If the webhook receiver crashes, all alerts are silently lost. The health check will restart it, but there's a gap.
7. **No cross-commodity consistency check.** If BTC has 100 events/hour but XAU has 0 (broken API for one symbol), no alert fires — only the BTC-specific heartbeat exists.

---

## 7. Security Model

### Positive Controls

| Control | Implementation |
|---------|---------------|
| No-new-privileges | `security_opt: no-new-privileges:true` on all containers |
| Capability drop | `cap_drop: ALL` on Spark, producer, alert-receiver, kafka-lag, retention, dbt-scheduler |
| Read-only rootfs | producer, alert-receiver, kafka-lag |
| Non-root users | Producer (appuser), alert-receiver (appuser), kafka-lag (appuser), Spark (uid 185 via setpriv), dbt-scheduler (uid 1000) |
| Webhook auth | Alert-receiver requires `ALERT_WEBHOOK_TOKEN` (mandatory unless explicitly disabled) |
| tmpfs /tmp | Producer, alert-receiver (no persistent writable disk) |
| Slim base images | python:3.12-slim |
| Trivy scanning | CI pipeline scans filesystem + 5 custom images (OS + library vulnerabilities) at HIGH/CRITICAL level |
| Pre-commit hooks | gitleaks (secret scanning) + ruff (Python linting) via `.pre-commit-config.yaml` |
| CI supply chain | All GitHub Actions SHA-pinned to prevent tag-based supply chain attacks |
| Trivy ignore policy | `.trivyignore` with expiry dates (`Expires: YYYY-MM-DD`) for quarterly review |
| Port binding | All external ports bound to 127.0.0.1 |
| RBAC | 5 distinct database roles with least-privilege grants |

### Security Gaps

| # | Severity | Issue |
|---|----------|-------|
| 1 | **CRITICAL** | All default passwords are `change_me` (.env.example). No enforcement of strong passwords or rotation policy. |
| 2 | **HIGH** | No TLS anywhere. Postgres: `sslmode=disable`. Kafka: PLAINTEXT only. Grafana webhook: HTTP. All credentials traverse the network in cleartext. |
| 3 | **HIGH** | `PGPASSWORD` for backup-user is exposed in environment (visible via `docker inspect`, `/proc`). |
| 4 | **MEDIUM** | No PostgreSQL audit logging (`log_statement` not configured). |
| 5 | **MEDIUM** | No Kafka authentication (SASL). Any service on the `messaging` network can produce/consume. |
| 6 | **LOW** | Data at rest unencrypted (pgdata volume, backup files, Kafka data). |
| 7 | **LOW** | No egress filtering. A compromised container can reach any external endpoint. |

**Assessment:** The security posture is appropriate for a single-machine development/thesis environment. It is **not production-ready** without TLS, credential management, and audit logging.

---

## 8. Deployment Instructions

### Prerequisites

- Docker Engine ≥ 24.0 with Compose V2
- 4 GB available RAM (minimum; 6 GB recommended)
- Twelve Data API key ([twelvedata.com](https://twelvedata.com))

### Initial Setup

```bash
# 1. Clone and configure
cd streaming_system
cp .env.example .env

# 2. Edit .env — REQUIRED changes:
#    - TD_API_KEY: Your Twelve Data API key
#    - All passwords: Change from "change_me" to strong values
#      (POSTGRES_PASSWORD, SPARK_DB_PASSWORD, DBT_DB_PASSWORD,
#       GRAFANA_DB_PASSWORD, PRODUCER_DB_PASSWORD, BACKUP_DB_PASSWORD,
#       GF_SECURITY_ADMIN_PASSWORD, PGADMIN_DEFAULT_PASSWORD)
nano .env

# 3. Start the system
make real    # Production: core services + ops (backup, lag monitor)
# OR
make dev     # Development: adds pgAdmin (5050) and Kafka UI (8080)
```

### Verify Deployment

```bash
# Check all services are healthy
make health

# Stream logs to verify data flow
make logs-core

# Expected startup sequence (~2 minutes):
#   1. Postgres initializes schemas (init.sql)
#   2. Kafka starts (KRaft, ~30s)
#   3. Producer begins polling API
#   4. Spark connects to Kafka, starts streaming
#   5. dbt-scheduler runs first build
#   6. Grafana becomes available at http://127.0.0.1:3000
```

### Useful Commands

```bash
make logs              # Stream all service logs
make logs-core         # Stream core services only
make health            # Show service health status
make ps                # Show container status

make dbt-build         # Manual dbt build
make dbt-deps          # Install dbt packages
make dbt-debug         # Validate dbt connection

make backup            # One-off database backup
make restore FILE=backup_YYYYMMDD_HHMM.dump  # Restore from backup

make down              # Stop all services (preserves data)
make downv             # Stop + delete volumes (DESTROYS ALL DATA)
```

### Post-Deployment Verification Checklist

1. **Grafana** — Open `http://127.0.0.1:3000`, login with configured admin credentials, verify market_overview dashboard shows data.
2. **Producer** — Check logs for successful API calls: `docker compose logs producer --tail=20`.
3. **Spark** — Verify batch processing: `docker compose logs spark-stream --tail=50`, look for `batch_id` log lines with `good_rows > 0`.
4. **dbt** — Verify models built: `make dbt-build`, check for success output.
5. **Alerts** — Check Grafana → Alerting → Alert Rules, verify all rules are in "Normal" state.
6. **Kafka** — (dev mode) Open `http://127.0.0.1:8080`, verify `commodity_prices` topic has messages.

### Volume Management

| Volume | Data | Recreatable? |
|--------|------|-------------|
| pgdata | All database data | **NO** — back up before `make downv` |
| kafka_data | Kafka logs/offsets | Yes (Spark replays from checkpoint) |
| spark_checkpoints | Spark offset state | Partially (earliest offset restarts from beginning, latest skips backlog) |
| spark_ivy_cache | Maven dependency cache | Yes (re-downloaded on start) |
| grafana_data | Grafana state | Yes (provisioned from config files) |
| ./backups | pg_dump archives | **NO** — stored on host filesystem |

---

## 9. CI/CD Pipeline

### Workflows

| Workflow | Trigger | Actions | Duration |
|----------|---------|---------|----------|
| `python-quality.yml` | Push to main, PRs | Ruff lint + pytest (SHA-pinned actions) | ~30s |
| `dbt-ci.yml` | Push to main, PRs | dbt build against ephemeral Postgres (31-row seed, 2-pass: full-refresh + incremental) | ~60s |
| `security-trivy.yml` | Push to main, PRs | Filesystem scan + 5 image scans (OS + library, HIGH/CRITICAL) | ~5m |

All GitHub Actions are SHA-pinned to prevent supply chain attacks. The `.trivyignore` file uses structured entries with `Added:` and `Expires:` dates for quarterly review.

### CI Weaknesses

1. **No integration test.** The CI never tests the actual pipeline (Producer → Kafka → Spark → Postgres → dbt). Each component is tested in isolation, if at all.
2. **No test coverage threshold.** pytest runs but has no minimum coverage gate. Test coverage includes producer utilities and Spark validation (27 tests in `tests/test_spark_validation.py`).
3. **No Docker Compose build verification.** `docker compose build` is never run in CI. A broken Dockerfile won't be caught until manual deployment.
4. **No branch protection enforced.** PRs can merge without passing CI checks.
5. **No image registry.** Images are built locally only; no versioned artifacts.

---

## 10. Known Limitations & Future Improvements

### Addressed Issues

The following issues were identified during development and have been resolved:

- DLQ write failures now logged with `DLQ_WRITE_FAILURE` tag and `lost_records=N` count
- All monitoring tables have 90-day retention in `retention.sql`
- Backoff multiplier resets to 1 after any successful API response
- Retention automated via dbt-scheduler (every 24h, 90-day TTL)
- Per-partition Kafka lag alert added (`kafka_partition_lag_warn_gt_30`)
- Advisory lock unlock with 3-attempt retry and explicit warning logging

### Production Readiness

| Priority | Issue | Context | Recommendation |
|----------|-------|---------|----------------|
| P1 | Default credentials in `.env.example` | Expected for dev/thesis setup; production would require unique secrets | Generate random passwords with a setup script; add `.env` validation on startup |
| P1 | No TLS on internal communication | Docker network provides isolation; sufficient for single-host thesis deployment | Enable Postgres SSL, Kafka SASL_SSL for multi-host production deployment |
| P2 | No integration tests | Unit tests and dbt tests cover individual components | Add docker-compose test mode with synthetic producer and end-to-end assertion |
| P2 | Single-node Kafka (RF=1) | Sufficient for thesis scope (3 instruments, 6-min intervals) | For production, deploy 3-node cluster with RF=3 |
| P3 | Price bounds and event thresholds are hardcoded | Works for current 3 instruments; would need config for more | Externalize to config file or dbt vars |
| P3 | No Kubernetes manifests | Single-machine deployment is thesis scope | Out of scope; document as future work |
| P3 | No structured logging | Plain-text logs are readable for thesis scale | Use Python `logging` with JSON formatter for production |
| P3 | `mart_latest_prices` view queries `stg_raw_prices` on every read | Performant at current data volume thanks to 24h optimization window | Consider materialized view or caching if query latency grows |

### Overall Assessment

The system demonstrates strong architectural foundations: idempotent data flow, role-based access control, checkpoint-based effectively-once semantics, commodity-aware analytics, and comprehensive alert coverage. The design choices are well-reasoned for the stated use case (3 instruments, 6-minute intervals, single-machine deployment).

The P1 items (credentials, TLS) are standard for development environments and would be addressed before any production deployment. The system includes 64 dbt tests, 11 Grafana alert rules, CI pipelines (lint, test, security scanning), and automated operational services (backup, retention, lag monitoring) — providing a robust foundation that exceeds typical thesis requirements.
