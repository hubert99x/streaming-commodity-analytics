# Technical Documentation — Commodity Price Streaming System

> Critical analysis. Last updated: 2026-03-13 (post-optimization revision).

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
│   REST API   │────▶│  (KRaft)   │────▶│    Streaming     │────▶│   16.6       │
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

### Service Inventory (15 total)

| Service | Image | Profile | Resource Limits |
|---------|-------|---------|-----------------|
| postgres | postgres:16.6 | always | 512MB / 1.0 CPU |
| kafka | confluentinc/cp-kafka:7.6.1 (KRaft) | always | 1GB / 1.0 CPU |
| spark-stream | apache/spark:3.5.1 | always | 1GB / 1.5 CPU |
| spark (debug) | apache/spark:3.5.1 | always | 1GB / 1.0 CPU |
| producer | python:3.11-slim (custom) | always | 128MB / 0.25 CPU |
| dbt-scheduler | dbt-postgres:1.9.0 (custom) | always | 256MB / 0.5 CPU |
| dbt (manual) | dbt-postgres:1.9.0 | always | 256MB / 0.5 CPU |
| grafana | grafana/grafana:11.0.0 | always | 256MB / 0.5 CPU |
| alert-receiver | python:3.12-slim (custom) | always | 128MB / 0.25 CPU |
| pgadmin | dpage/pgadmin4:8.14 | dev | 256MB / 0.5 CPU |
| kafka-ui | provectuslabs/kafka-ui:v0.7.2 | dev | 256MB / 0.5 CPU |
| kafka-lag | python:3.12-slim (custom) | ops | 128MB / 0.25 CPU |
| retention | postgres:16.6 (custom) | ops | 128MB / 0.25 CPU |
| backup-cron | postgres:16.6 | ops | 256MB / 0.5 CPU |

### Network Topology

Three isolated Docker bridge networks enforce segmentation:

```
┌─── database ──────────────────────────────────────────────────────┐
│  postgres, spark-stream, spark, dbt, dbt-scheduler, grafana,     │
│  pgadmin, kafka-lag, alert-receiver, retention, backup-cron      │
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
 │  analytics.mart_latest_prices    (table)     │
 │  analytics.mart_minute_last_price (incr.)    │
 │  analytics.mart_price_events      (incr.)    │
 │  analytics.mart_price_volatility_1h (incr.)  │
 └──────────────────┬──────────────────────────-┘
                    │
                    ▼
 ┌─────────────────────────────────────────────┐
 │        GRAFANA (3 dashboards, 7 alerts)      │
 │  market_overview, market_analysis,           │
 │  pipeline_&_data_quality                     │
 │                                              │
 │  Alert webhook → alert-receiver → Postgres   │
 └─────────────────────────────────────────────-┘
```

### Exactly-Once Delivery Analysis

The pipeline achieves **effective exactly-once** semantics through layered idempotency:

| Layer | Mechanism | Guarantee |
|-------|-----------|-----------|
| Producer → Kafka | `enable.idempotence=True`, `acks=all`, deterministic UUID5 event_id | At-least-once (Kafka deduplicates producer retries) |
| Kafka → Spark | Checkpoint-based offset tracking (not consumer groups) | At-least-once (replays from checkpoint on crash) |
| Spark → Postgres | `ON CONFLICT (event_id) DO NOTHING` | At-most-once per event_id (duplicates silently dropped) |
| **Combined** | | **Effectively exactly-once at Postgres level** |

**Weakness:** There are no metrics for ON CONFLICT discards. If Spark replays a batch after crash recovery, the duplicate rows are silently dropped. There is no way to distinguish "healthy idempotent skip" from "data quality problem causing ID collisions." A counter for conflict-skipped rows would improve observability.

---

## 3. Component Descriptions

### 3.1 Producer (`producer/producer.py`)

**Role:** Polls Twelve Data REST API every 6 minutes, publishes price events to Kafka.

**Key behaviors:**
- **Deterministic event IDs:** UUID5 (DNS namespace + `commodity:ISO_timestamp`). Same commodity+timestamp always produces same ID, preventing semantic duplicates across retries.
- **FX weekend gating:** XAU/USD and EUR/USD are not fetched Fri 22:00 – Sun 21:59 UTC. BTC runs 24/7.
- **Three-tier backoff:** Rate limit (429) → fixed backoff; server error (5xx) → exponential backoff with multiplier (1→2→4→...32); other errors → interval-length backoff. All clamped to 15–600s range.
- **Kafka producer config:** `enable.idempotence=True`, `acks=all`, `retries=10`, `linger.ms=0`.
- **Pre-publish price bounds validation:** Checks prices against commodity-specific bounds (XAU: 500–15000, BTC: 100–1M, EUR: 0.5–2.0) before publishing to Kafka. Out-of-bounds prices are logged and skipped, preventing pipeline contamination at the source.
- **API metrics:** Each API call is logged to `monitoring.api_calls` (status code, latency, error message) via a lazy Postgres connection.
- **Graceful shutdown:** SIGINT/SIGTERM handlers flush the Kafka producer buffer before exit.

**Weaknesses:**
- **Backoff multiplier never resets on success.** After a single transient 5xx error, the multiplier increments. Even if the next request succeeds, subsequent failures start from the elevated multiplier. Only a 429 rate-limit resets it. This can cause prolonged polling gaps after brief network hiccups.
- **No circuit breaker.** Exponential backoff can reach 10+ minutes. There is no alert mechanism if the producer enters prolonged backoff — the system just goes quiet.
- **Global mutable `_pg_conn`.** Safe in the current single-threaded design, but will silently corrupt if the producer is ever made concurrent.

### 3.2 Spark Structured Streaming (`spark/stream_to_postgres.py`)

**Role:** Consumes from Kafka, validates records, routes bad records to DLQ, writes good records to Postgres.

**Key behaviors:**
- **Trigger:** 300-second processing intervals. `maxOffsetsPerTrigger=5000` limits backpressure.
- **Offset management:** Checkpoint directory (not Kafka consumer groups). Each Spark instance maintains its own offset state.
- **Validation pipeline:** Multi-level checks per record (logic extracted to `spark/validation.py` for testability — 27 unit tests):
  - Null field detection (MISSING_FIELD errors)
  - Price positivity check
  - Schema version check (must be `1`)
  - Commodity-specific price bounds (XAU: 500–15000, BTC: 100–1M, EUR: 0.5–2.0)
- **Staging table pattern:** Good records → `ingest.raw_prices_staging` (truncate-append-merge). PostgreSQL advisory lock (key 1) serializes concurrent merges.
- **DLQ:** Bad records → `ingest.dlq_staging` → merged into `monitoring.dead_letter_events` with its own advisory lock (key 2) and unique constraint to prevent duplicates on batch replay.
- **JDBC timeouts:** `connectTimeout=10s`, `socketTimeout=30s` prevent indefinite hangs on Postgres connection issues.
- **Health check:** Docker liveness probe (`ps aux | grep spark-submit`) detects process crashes.
- **Deduplication:** Handled entirely by PostgreSQL `ON CONFLICT (event_id) DO NOTHING` — no in-batch `dropDuplicates` needed.

**Weaknesses:**
- **DLQ write failures are silent.** If the DLQ staging insert fails, the error is logged to stdout but the bad records are permanently lost. No alert fires; no retry occurs.
- **Price bounds are hardcoded.** Changing thresholds (e.g., if gold exceeds $15,000) requires a code change and container rebuild. No external configuration mechanism exists.
- **Advisory lock unlock failures are swallowed.** If `pg_advisory_unlock` raises an exception (line 191), it is caught and logged but the lock remains held until the JDBC connection closes. If the connection persists, subsequent batches will deadlock waiting for the lock.
- **Concurrent instance race.** If two Spark instances start during a restart (old shutting down, new starting up), both read the same checkpoint and process overlapping offset ranges. Advisory locks protect the staging merge but not the consumption itself. Idempotency at the Postgres layer saves correctness, but batch metrics become misleading (double-counted).
- **`failOnDataLoss=false`** means Kafka offset gaps (e.g., from topic retention) are silently ignored. No alert fires when Spark skips over lost offsets.

### 3.3 dbt Scheduler (`ops/dbt-scheduler/`)

**Role:** Runs `dbt build` every 6 minutes and `dbt test` (with result logging) every 30 minutes.

**Key behaviors:**
- Python-based scheduler with `threading.Lock()` to prevent overlapping runs.
- Health heartbeat: writes timestamp to `/tmp/dbt_scheduler_alive`, checked by Docker health probe with 10-minute tolerance.
- `dbt test` results parsed from `target/run_results.json` via `jq` and inserted into `monitoring.dbt_test_runs`.
- 300-second timeout on subprocess execution.
- **Automated ingest cleanup:** Periodically runs `cleanup_ingest_keep_2000.sql` to prune orphaned staging tables (configurable via `INGEST_CLEANUP_INTERVAL`, default 3600s).
- **Non-root execution:** Runs as UID 1000 (`USER 1000` in Dockerfile). Container hardened with `cap_drop: ALL`.

**Weaknesses:**
- **If dbt build consistently exceeds 6 minutes, runs are silently skipped** (lock contention). There is no alert for "dbt build took too long" — only the file-marker health check would eventually fail after 10 minutes of no heartbeat.
- **No build duration tracking.** There is no metric for how long each dbt build takes. Progressive slowdown (from table growth) would go unnoticed until it exceeds the 5-minute timeout.

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

**Retention (`retention`):** Manual trigger (restart: "no"). Deletes records older than 90 days from `raw_prices`, `dead_letter_events`, `alert_events`. Runs `VACUUM ANALYZE` on Sundays only.

**Weaknesses:**
- **Retention is not automated.** Unlike backup-cron, the retention service must be manually invoked. Without regular cleanup, `raw_prices` grows unbounded (backups still rotate, but the live table doesn't shrink).
- **Monitoring tables have no retention.** `api_calls`, `kafka_lag`, `dbt_test_runs`, `backup_log` are never purged and grow indefinitely.
- **Backups are unencrypted.** Stored as plain `pg_dump` files on the host filesystem. No encryption at rest.
- **No restore testing.** No automated verification that backups can be successfully restored.

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
│   ├── mart_latest_prices      ← Table (latest price per commodity)
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
| `dbt_runner` | public, analytics | SELECT raw_prices, CREATE analytics models |
| `grafana_read` | analytics, monitoring | SELECT only (auto-granted on new dbt objects) |
| `producer_writer` | monitoring | INSERT api_calls |
| `backup_user` | all | Superuser for pg_dump |

**Notable:** `DEFAULT PRIVILEGES FOR USER dbt_runner` auto-grants SELECT to `grafana_read` on any new table dbt creates. This is a well-designed pattern that prevents missing grants when models are added.

**Weakness:** The `backup_user` role uses superuser privileges and passes the password via `PGPASSWORD` environment variable, which is visible in `docker inspect` and `/proc` on the host. A `.pgpass` file with restricted permissions would be more secure.

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
       ├──▶ mart_latest_prices      (TABLE, full rebuild)
       ├──▶ mart_minute_last_price  (INCREMENTAL, 30m lookback)
       ├──▶ mart_price_events       (INCREMENTAL, 2h lookback)
       └──▶ mart_price_volatility_1h (INCREMENTAL, 2h lookback)
```

### Model Details

#### `stg_raw_prices` (View)
Pass-through with explicit type casts and timezone normalization (`timestamptz` → naive UTC). Preserves Kafka partition/offset for lineage tracing. Defined via `{{ source('public', 'raw_prices') }}` with **source freshness SLA** (warn after 10 minutes, error after 20 minutes).

#### `mart_latest_prices` (Table, full rebuild)
One row per commodity. Uses PostgreSQL `DISTINCT ON` with a 24-hour optimization window — scans last 24 hours first, falls back to full scan only for commodities missing from that window.

#### `mart_minute_last_price` (Incremental, 30-min lookback)
1-minute OHLC-style buckets. Picks the **last** price per minute via `array_agg(price ORDER BY event_ts DESC)[1]`. Includes event count (`n`), min/max price. Post-hook creates a `(symbol, minute_bucket DESC)` index.

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

### Data Quality Tests (17+)

- **Staging:** not_null and unique on event_id; accepted_values on commodity and currency; freshness bounds (`event_ts` within -2h to +1min of `ingest_ts`).
- **Marts:** unique combination checks on composite keys; price sanity (> 0, min ≤ max); event_type accepted values.
- **Custom SQL test:** `test_price_jump.sql` — detects unrealistic minute-to-minute jumps per commodity (EUR >5%, XAU >10%, BTC >30%).

**Weaknesses:**
- **Incremental lookback edge case.** If `dbt build` is skipped for >30 minutes (scheduler blocked or container restarting), `mart_minute_last_price`'s 30-minute lookback window may miss late-arriving data from before the gap.
- **`mart_latest_prices` is fully rebuilt each run.** As `raw_prices` grows, this will become slower. The 24-hour optimization helps but has a fallback full scan for any commodity missing from the window.
- **Hardcoded thresholds.** Price event thresholds and bounds are embedded in SQL. Changing them requires a dbt rebuild and potential reprocessing of the incremental lookback window.

---

## 6. Monitoring & Alerting

### Alert Rules (7 total, 30-second evaluation)

| Alert | Condition | Severity | Fires After |
|-------|-----------|----------|-------------|
| Stale Ingest (>7m) | `time_since_last_ingest_seconds > 420` | CRITICAL | 2 min |
| BTC Events Low (15m) | `btc_events_last_15m < 2` | WARNING | 2 min |
| API Errors ≥1 (18m) | `errors_18m >= 1` | WARNING | 2 min |
| API Errors ≥3 (18m) | `errors_18m >= 3` | CRITICAL | 2 min |
| DLQ Events (15m) | DLQ count > 0 in 15m window | WARNING | 2 min |
| Kafka Lag >50 | `total_lag > 50` | WARNING | 2 min |
| Kafka Lag >500 | `total_lag > 500` | CRITICAL | 2 min |

**Routing:** Critical alerts → `postgres-webhook` contact point → alert-receiver → `monitoring.alert_events`.

**`noDataState`:** Most alerts fire on no-data (no data = something is broken), except DLQ (no data = no bad records = OK).

### Monitoring Views (materialized as SQL views)

- **`pipeline_metrics`:** Single-row summary with `time_since_last_ingest_seconds`, `events_last_6m`, `btc_events_last_15m`.
- **`api_metrics_18m`:** Rolling 18-minute API success rate.
- **`kafka_lag_latest`:** Latest lag per consumer group via `DISTINCT ON`.

### Monitoring Gaps

1. ~~**No dbt health alert.**~~ **Partially addressed.** dbt source freshness SLA (warn 10m, error 20m) detects stale `raw_prices` input. However, there is still no alert for dbt build *failures* — if dbt crashes but `raw_prices` keeps flowing, no alert fires.
2. **No dbt build duration tracking.** Progressive slowdown from table growth would go unnoticed.
3. **No per-partition Kafka lag.** Only total lag is alerted on. A single stuck partition could be masked by healthy partitions.
4. **No Spark streaming metrics.** Microbatch duration, watermark lag, and task counts are not exposed.
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
| Read-only rootfs | Spark, producer, alert-receiver, kafka-lag |
| Non-root users | Producer (appuser), alert-receiver (appuser), kafka-lag (appuser), Spark (uid 185 via setpriv), dbt-scheduler (uid 1000) |
| Webhook auth | Alert-receiver requires `ALERT_WEBHOOK_TOKEN` (mandatory unless explicitly disabled) |
| tmpfs /tmp | Producer, alert-receiver (no persistent writable disk) |
| Slim base images | python:3.11-slim, python:3.12-slim |
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

## 10. Critical Weaknesses & Recommendations

### Severity: Critical

| # | Issue | Impact | Recommendation |
|---|-------|--------|----------------|
| 1 | **Default credentials everywhere** | Complete system compromise if deployed as-is | Generate random passwords with a setup script; add `.env` validation on startup |
| 2 | **No TLS on any internal communication** | Credential sniffing, data interception on shared hosts | Enable Postgres SSL, Kafka SASL_SSL; use reverse proxy with TLS for Grafana |
| 3 | **No integration tests** | Silent pipeline breakage not caught before deployment | Add docker-compose test mode with synthetic producer and end-to-end assertion |

### Severity: High

| # | Issue | Impact | Recommendation |
|---|-------|--------|----------------|
| 4 | **DLQ write failures are silent** | Permanent data loss for malformed records with no alert | Add DLQ write failure counter; alert on non-zero |
| 5 | **Monitoring tables grow unbounded** | Disk exhaustion over time | Add retention policy for all monitoring tables (30-90 day TTL) |
| 6 | **Backoff multiplier doesn't reset on success** | Prolonged polling gaps after transient errors | Reset multiplier to 1 after any successful API response |

### Severity: Medium

| # | Issue | Impact | Recommendation |
|---|-------|--------|----------------|
| 7 | **Single-node Kafka (RF=1)** | Any Kafka failure = full pipeline outage + potential data loss | Document as known limitation; for production, deploy 3-node cluster |
| 8 | **Retention service is manual** | raw_prices grows unbounded unless operator remembers to run retention | Automate via cron or integrate into backup-cron schedule |
| 9 | **No per-partition Kafka lag alert** | Single stuck partition masked by total lag metric | Add `max_partition_lag` threshold alert |
| 10 | **Price bounds and event thresholds are hardcoded** | Changing market conditions require code changes + rebuilds | Externalize to config file or dbt vars |
| 11 | **Advisory lock unlock failure silently swallowed** | Potential batch-level deadlock until connection close | Add retry logic and explicit logging |

### Severity: Low

| # | Issue | Impact | Recommendation |
|---|-------|--------|----------------|
| 12 | No Kubernetes manifests | Cannot scale beyond single machine | Out of scope for thesis; document limitation |
| 13 | Grafana dashboards are JSON (not version-controlled YAML) | Brittle to edit, hard to diff | Acceptable for provisioned dashboards |
| 14 | No structured logging | Harder to aggregate logs across services | Use Python `logging` with JSON formatter |
| 15 | `mart_latest_prices` full rebuild every 6m | Will slow down as raw_prices grows | Add incremental strategy or materialized view |

### Overall Assessment

The system demonstrates strong architectural foundations: idempotent data flow, role-based access control, checkpoint-based exactly-once semantics, commodity-aware analytics, and comprehensive alert coverage. The design choices are well-reasoned for the stated use case (3 instruments, 6-minute intervals, single-machine deployment).

The primary weaknesses cluster around **operational maturity** (no TLS, default credentials, unbounded monitoring table growth) and **test coverage** (no integration tests, no CI composition verification). These are consistent with a thesis/prototype system and would need to be addressed before any production deployment.

Recent improvements have addressed several previously critical gaps: Spark health checks, dbt source freshness monitoring, non-root container execution across all services, mandatory webhook authentication, pre-publish price validation, JDBC timeouts, CI supply chain security (SHA-pinned actions), and expanded dbt CI seed data with two-pass incremental testing.

For the thesis context, the most impactful remaining improvements would be:
1. Adding an integration test that exercises the full pipeline end-to-end
2. Automating the retention service (preventing the most likely operational failure)
3. Adding DLQ write failure alerting (closing the last silent data loss path)
