# Near Real-Time Commodity Price Streaming System

Traditional batch pipelines delay market change detection by minutes to hours. This system provides near real-time commodity price analytics with a micro-batch streaming architecture, layered reliability, and built-in observability.

Ingests XAU/USD, BTC/USD, and EUR/USD prices every 6 minutes from Twelve Data API, streams through Kafka, processes with Spark Structured Streaming into PostgreSQL, transforms with dbt, and visualizes in Grafana.

The system emphasizes reliability through idempotent processing at every layer, a dead letter queue for invalid records, checkpoint-based fault-tolerant recovery, 11 automated alert rules, and 64 dbt data quality tests.

## Architecture

### Core Data Flow
![Core Data Flow](docs/core_data_flow.png)

### Monitoring & Operations
![Monitoring & Operations](docs/monitoring_operations.png)

## Why This Project

This is a master's thesis project that demonstrates end-to-end data engineering beyond a simple ETL script. It covers: event-driven ingestion, micro-batch stream processing, layered idempotency, dead letter queues, data quality testing, observability with alerting, container hardening, and CI/CD — all wired together as a single `make real` deployment.

## Key Design Decisions

| Decision | Why |
|----------|-----|
| **Idempotent inserts** (`ON CONFLICT DO NOTHING`) | Deterministic event IDs (UUID5) mean the same event always produces the same ID — enables safe replay after crashes without duplicates |
| **Dead Letter Queue** | Invalid records land in `monitoring.dead_letter_events` with full payload — nothing is silently dropped |
| **Checkpoint-based offsets** | Spark manages Kafka offsets via checkpoint dir, not consumer groups — avoids offset conflicts on restart |
| **Persistent staging + advisory lock** | Batch data lands in a staging table, then merges into target under `pg_advisory_lock` — serializes concurrent writes without table-level locks |
| **Multi-layer validation** | Price bounds checked at producer (pre-publish) AND Spark (post-consume) — defense in depth |
| **FX weekend gating** | XAU/USD, EUR/USD skipped Fri 22:00 – Sun 21:59 UTC; BTC runs 24/7 — prevents stale quotes from polluting analytics |
| **5 database roles** | Each service gets only the permissions it needs — a compromised component cannot escalate beyond its own schema |
| **Incremental dbt models** | Marts use lookback windows (30m–2h) — constant runtime regardless of table size |
| **Per-commodity event thresholds** | BTC extreme = 1.5%, XAU = 0.6%, EUR = 0.25% — reflects actual market volatility profiles |

## Services

| Service | Role | Profile |
|---------|------|---------|
| **postgres** | PostgreSQL 16.6 — primary database | core |
| **kafka** | KRaft mode, 3 partitions | core |
| **producer** | Fetches prices from Twelve Data API every 6 min | core |
| **spark-stream** | Kafka → PostgreSQL via Structured Streaming (trigger 300s) | core |
| **dbt-scheduler** | `dbt run` every 6m, `dbt test` every 30m, retention every 24h | core |
| **grafana** | 3 dashboards, 11 alert rules | core |
| **alert-receiver** | Flask webhook for Grafana alerts → PostgreSQL | core |
| **kafka-lag** | Monitors Spark consumer lag | ops |
| **backup-cron** | pg_dump every 2h, keeps last 360 backups | ops |
| **retention** | Manual retention cleanup (alternative to scheduler retention) | ops |
| **spark** | Interactive Spark shell for debugging | manual |
| **dbt** | One-off dbt commands | manual |
| **pgadmin** | Database admin UI (port 5050) | dev |
| **kafka-ui** | Kafka topic browser (port 8080) | dev |

## Database & Transformations

**4 schemas:** `public` (Spark sink), `analytics` (dbt models), `monitoring` (operational metrics), `ingest` (persistent Spark staging tables, truncated between batches)

**dbt models:**

| Model | Type | Description |
|-------|------|-------------|
| `stg_raw_prices` | view | Type casting, timezone handling |
| `mart_latest_prices` | view | Latest price per instrument |
| `mart_minute_last_price` | incremental | Minute-level OHLC statistics |
| `mart_price_events` | incremental | Significant price changes with per-commodity thresholds |
| `mart_price_volatility_1h` | incremental | Hourly volatility metrics |

## Monitoring

**3 Grafana dashboards** (auto-provisioned): Market Overview, Market Analysis, Pipeline & Data Quality

**11 alert rules** covering: stale ingest, API errors, DLQ events, dbt test failures, Kafka lag (total + per-partition), BTC heartbeat, backup freshness, analytics staleness. All alerts route through webhook receiver → `monitoring.alert_events`.

**6 monitoring tables:** `api_calls`, `dead_letter_events`, `kafka_lag`, `alert_events`, `dbt_test_runs`, `backup_log` + 3 summary views (`pipeline_metrics`, `api_metrics_18m`, `kafka_lag_latest`).

This ensures that pipeline failures (ingestion gaps, data quality regressions, consumer lag) are detected automatically and logged for post-mortem analysis.

See [Operations Guide](docs/OPERATIONS.md#alert-rules-11-rules) for full alert rule details.

## CI/CD

| Workflow | Trigger | Description |
|----------|---------|-------------|
| `python-quality.yml` | Push/PR | Ruff lint + pytest (Python 3.11) |
| `dbt-ci.yml` | Push/PR | dbt build against ephemeral Postgres (Python 3.12) |
| `security-trivy.yml` | Push/PR + weekly | Trivy filesystem & image scanning |

## Security

The project applies several production-inspired hardening practices (container capability drop, non-root users, read-only rootfs, RBAC with 5 database roles, pre-commit secret scanning, SHA-pinned CI actions), but it is designed as a single-host educational system rather than a fully production-grade distributed deployment.

See [Security](docs/SECURITY.md) for full details.

## Quick Start

### Prerequisites

| Software | Version |
|----------|---------|
| Docker | 24.0+ |
| Docker Compose | v2.0+ (plugin) |
| Git | 2.30+ |
| Make | any |

**System:** 4+ CPU cores, 6 GB+ RAM, 10 GB disk. **API key:** register at [twelvedata.com](https://twelvedata.com/) (free tier is sufficient).

### Setup

```bash
# 1. Clone
git clone https://github.com/hubert99x/streaming-commodity-analytics.git
cd streaming-commodity-analytics

# 2. Configure
cp .env.example .env
# Edit .env — set TD_API_KEY at minimum, review passwords

# 3. Start
make real

# 4. Verify
make health          # all services should show "healthy" within ~1 min

# 5. Open Grafana at http://localhost:3000
```

### What to Expect After Startup

- Producer polls Twelve Data API every **6 minutes**
- Spark processes micro-batches every **5 minutes** (trigger interval)
- dbt transforms run every **6 minutes**, tests every **30 minutes**
- First data appears in Grafana after **~6–12 minutes** (first poll + Spark trigger)
- On weekends, only BTC/USD updates continuously — XAU/USD and EUR/USD are gated (Fri 22:00 – Sun 21:59 UTC)
- You can verify ingestion directly in PostgreSQL (`public.raw_prices`) — see [Troubleshooting](docs/TROUBLESHOOTING.md#diagnostics)
- Kafka topic activity can be inspected via Kafka UI (`make dev`, port 8080)

## Project Structure

```
streaming-commodity-analytics/
├── producer/              # Python API producer
├── spark/                 # Spark Structured Streaming job
├── dbt/                   # dbt models (staging + marts)
├── ops/                   # Operational services
│   ├── alert-receiver/    #   Flask webhook listener
│   ├── dbt-scheduler/     #   Automated dbt runs
│   ├── kafka-lag/         #   Consumer lag monitor
│   └── sql/               #   Init schema, grants, retention SQL
├── grafana/
│   ├── dashboards/        #   3 provisioned dashboard JSONs
│   └── provisioning/      #   Datasource, dashboard, alerting config
├── tests/                 #   Unit tests (pytest)
├── docs/                  #   Architecture diagrams, technical docs
├── .github/workflows/     #   CI pipelines
├── docker-compose.yml     #   All service definitions
├── Makefile               #   Common commands
└── .env.example           #   Environment variable template
```

## Documentation

| Document | Description |
|----------|-------------|
| [Operations Guide](docs/OPERATIONS.md) | Commands, dashboards, alert rules, checkpoints, volume management |
| [Troubleshooting](docs/TROUBLESHOOTING.md) | Common issues, diagnostics, DLQ investigation |
| [Security](docs/SECURITY.md) | Container hardening, RBAC, CI scanning |
| [Disaster Recovery](docs/DISASTER_RECOVERY.md) | Backup/restore procedures, volume management |
| [Technical Documentation](docs/TECHNICAL_DOCUMENTATION.md) | Deep-dive architecture analysis, weaknesses, recommendations |

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
