# Troubleshooting

## Common Issues

| Problem | Cause | Solution |
|---------|-------|---------|
| No new data in raw_prices | API failure or rate limit | Check `monitoring.api_calls` and producer logs |
| No data in Grafana (initial startup) | Price/status panels read views, charts read incremental marts | Allow ~15 min: 6 min poll + 5 min Spark trigger + 6 min dbt build |
| Charts empty while price panels show a value | `mart_latest_prices` is a view (instant); `mart_minute_last_price` is an incremental table (needs a dbt build) | Wait one dbt cycle (6 min) |
| Pipeline Latency above 360s right after a restart | Spark is draining the Kafka backlog accumulated while it was down | Expected – settles within a few micro-batches |
| API P95 Latency shows tens of seconds | The panel window is 60 min and holds only ~10 calls, so one slow call dominates the percentile | Check `monitoring.api_calls` for `latency_ms > 10000`; the value ages out of the window |
| Spark crash-loop | Dependency or Python compatibility issue | Check Spark logs and validation/typing issues in code |
| Kafka Lag increasing (growing backlog) | Spark not keeping up or stuck | Check Spark logs and `monitoring.kafka_lag_latest` |
| High Pipeline Latency | Producer (360s) + Spark (300s) cycles are not synchronized | Expected – worst-case latency ~660s, typical lower |
| dbt test FAIL | Temporary data inconsistency (e.g. restart, delayed ingest, freshness breach) | Usually auto-resolves; check `monitoring.dbt_test_runs` |
| Pipeline Status = WARNING | Weekend with 1 instrument (BTC only) | Normal – XAU/EUR are gated on weekends |

## Quick Debug Order

1. Check `monitoring.pipeline_metrics` – overall pipeline health (ingest delay, events count)
2. Check `monitoring.api_calls` – API availability (200 vs 4xx/5xx errors)
3. Check `public.raw_prices` – confirm data is being written (`event_ts` vs `ingest_ts`)

## Diagnostics

**Connect to PostgreSQL shell:**
```bash
docker compose exec postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

**Quick pipeline health check:**
```sql
SELECT * FROM monitoring.pipeline_metrics;
```

**Check recent API status:**
```sql
SELECT http_status, count(*) AS n
FROM monitoring.api_calls
WHERE ts_utc >= now() - interval '15 minutes'
GROUP BY 1
ORDER BY n DESC;
```

**Check if Spark is writing data:**
```bash
docker compose exec postgres sh -lc \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
   SELECT count(*) AS total_rows,
          max(event_ts) AS newest_event,
          max(ingest_ts) AS newest_ingest
   FROM public.raw_prices;"'
```

**Check recent DLQ events:**
```sql
SELECT count(*) AS dlq_last_1h
FROM monitoring.dead_letter_events
WHERE ts_utc >= now() - interval '1 hour';
```

**Check Kafka topic offsets (should increase over time):**
```bash
docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server kafka:29092 \
  --topic commodity_prices
```

## DLQ Investigation

When DLQ Events > 0 in Grafana, connect to PostgreSQL (see [Diagnostics](#diagnostics)) and check what's failing:
```sql
SELECT error_reason, count(*) AS n
FROM monitoring.dead_letter_events
WHERE ts_utc >= now() - interval '24 hours'
GROUP BY 1
ORDER BY n DESC;
```
