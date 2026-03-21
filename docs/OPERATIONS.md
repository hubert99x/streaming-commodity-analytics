# Operations Guide

## Commands

### Running the System
```bash
make real              # Production: core + ops services
make dev               # Development: adds pgAdmin (5050), Kafka UI (8080)
make down              # Stop all services
make downv             # Stop + remove volumes (DESTROYS Postgres data!)
make health            # Show service health
make logs              # Stream all logs
make logs-core         # Stream core service logs only
make restart           # Restart all services
make ps                # Show service status
```

### dbt
```bash
make dbt-build         # Run dbt build (models + tests) in container
make dbt-deps          # Install dbt packages
make dbt-debug         # Validate dbt profile and connection
```

### Testing & Linting (local)
These run automatically in CI on push/PR, but can also be run locally:
```bash
pytest -q              # Run unit tests (38 tests)
ruff check producer tests ops spark  # Lint Python code
```

### Backup & Restore
```bash
make backup                                  # One-off pg_dump
make restore FILE=backup_YYYYMMDD_HHMM.dump  # Restore from dump
```

## Common Operations

**Restart a single service:**
```bash
docker compose restart spark-stream
```

**View logs for specific services:**
```bash
docker compose logs -f --tail=200 producer spark-stream postgres
```

**Reset Kafka topic** (delete all messages and start fresh):
```bash
make down
docker volume rm streaming_system_kafka_data
docker volume rm streaming_system_spark_checkpoints
make real
```

**Force dbt rebuild** (full refresh, not incremental):
```bash
docker compose exec dbt sh -lc 'cd /dbt && dbt run --full-refresh'
```

## pgAdmin Access

```bash
make dev                    # starts dev profile (includes pgAdmin)
```
Open [http://localhost:5050](http://localhost:5050), login with credentials from `.env`.

## Spark Checkpoints

Spark uses a checkpoint directory (`spark_checkpoints` Docker volume) to track Kafka offsets and streaming state. Combined with idempotent writes (`ON CONFLICT DO NOTHING`), this provides effectively-once results in PostgreSQL across container restarts.

**When NOT to clear checkpoints:**
- Changing dbt models or SQL
- Changing Grafana dashboards or alerts
- Changing retention policy or producer logic (without schema change)
- Restarting containers

**When to clear checkpoints:**
- Changing the JSON schema produced to Kafka (e.g. adding/removing fields)
- Changing the Spark `StructType` schema definition in `stream_to_postgres.py`
- Changing the output columns written to PostgreSQL

**How to clear checkpoints:**
```bash
make down
docker volume rm streaming_system_spark_checkpoints  # remove checkpoint volume
make real
```
Spark will re-read from the earliest available Kafka offset and reprocess. Idempotent inserts (`ON CONFLICT DO NOTHING`) prevent duplicates.

## Alert Rules (11 rules)

All alerts evaluate every 30s and require the condition to persist for 2 minutes before firing. Alerts are sent to a Flask webhook receiver (`alert-receiver:5000`) which logs them to `monitoring.alert_events`.

| Rule | Severity | Condition |
|------|----------|-----------|
| Time Since Last Ingest > 7m | critical | No new rows in `raw_prices` for 420s |
| BTC events (15m) < 2 | warning | BTC is 24/7 — fewer than 2 events means pipeline stall |
| API errors (18m) >= 1 | warning | Any API error in last 3 poll cycles |
| API errors (18m) >= 3 | critical | Sustained API failures |
| DLQ events (15m) > 0 | warning | Malformed records detected |
| dbt test failures (35m) | warning | Any dbt test run with `status=FAIL` in last 35 minutes |
| Kafka lag > 50 | warning | Consumer falling behind |
| Kafka lag > 500 | critical | Severe backlog |
| Kafka partition lag > 30 | warning | Single stuck partition (may be masked by healthy total lag) |
| No backup in 25h | warning | No successful backup in `backup_log` for >25 hours |
| Stale mart_latest_prices (>15m) | warning | Analytics mart not refreshed in >15 minutes |

## Grafana Dashboards

All three dashboards are auto-provisioned from JSON files in `grafana/dashboards/`.

**Market Overview** — near real-time operational view of the entire pipeline:
- Live price charts (XAU/USD, BTC/USD, EUR/USD)
- Pipeline Health: API Idle Time, Events per Cycle, Kafka Consumer Lag, Ingest Idle Time
- API Metrics: calls count, errors, P95 latency, success rate

**Market Analysis** — analytical view of price behavior:
- Price Statistics table (latest price, min/max, range, std dev per instrument)
- Hourly Price Change (%) time series
- Recent Price Events table (MEDIUM / LARGE / EXTREME moves)

**Pipeline & Data Quality** — monitoring and diagnostics:
- Pipeline Status & Latency, Throughput per cycle (bar chart)
- DLQ: events count (24h), DLQ Rate, DLQ Log table, DLQ Events per Day (7d)
- dbt: test freshness (every 30min), pass rate, test runs table
- Backup freshness (every 2h), Time Since Last Stream Write

## Volume Management

| Volume | Data | Recreatable? |
|--------|------|-------------|
| pgdata | All database data | **NO** — back up before `make downv` |
| kafka_data | Kafka logs/offsets | Yes (Spark replays from checkpoint) |
| spark_checkpoints | Spark offset state | Partially (earliest restarts from beginning, latest skips backlog) |
| spark_ivy_cache | Maven dependency cache | Yes (re-downloaded on start) |
| grafana_data | Grafana state | Yes (provisioned from config files) |
| ./backups | pg_dump archives | **NO** — stored on host filesystem |
