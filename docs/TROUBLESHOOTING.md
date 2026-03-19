# Troubleshooting

## Common Issues

| Problem | Cause | Solution |
|---------|-------|---------|
| No data in Grafana | System needs ~6 min for first data | Wait for first poll cycle + Spark trigger |
| Pipeline Status = WARNING | Weekend with 1 instrument (BTC only) | Normal — XAU/EUR are gated on weekends |
| dbt test FAIL | System restart caused `ingest_ts - event_ts > 24h` | Will auto-resolve; see `_staging.yml` tolerance |
| Spark crash-loop | Python version compatibility | Check `spark/validation.py` uses `typing.Dict` not `dict[]` |
| High Pipeline Latency | Producer (360s) + Spark (300s) desync | Normal — max latency ~660s on weekends |

## Diagnostics

**Connect to PostgreSQL shell:**
```bash
docker compose exec postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
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

**Check if Kafka topic exists and has data:**
```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --describe --topic commodity_prices
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
