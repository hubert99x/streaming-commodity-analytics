# Disaster Recovery

## Backup Schedule

Automated backups run every 2 hours via `backup-cron` service (`ops` profile). Each backup is a `pg_dump -F c` archive stored in `./backups/` on the host. Last 360 backups are retained (~30 days). Backups are not encrypted and are stored locally on disk.

**Recovery Point Objective (RPO):** up to 2 hours of data loss  
**Recovery Time Objective (RTO):** a few minutes (restore + service restart)

## Manual Backup

```bash
make backup
```

## Restore from Backup

**List available backups:**
```bash
docker compose exec postgres sh -lc 'ls -1t /backups/*.dump | head -n 5'
```

**Restore** (simplest approach):
```bash
make restore FILE=backup_YYYYMMDD_HHMM.dump
docker compose restart spark-stream grafana dbt-scheduler
```

If restore fails with "cannot drop schema" errors, drop schemas first. This removes existing schema conflicts that prevent restore from completing:
```bash
docker compose exec postgres sh -lc \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
   DROP SCHEMA IF EXISTS analytics CASCADE;
   DROP SCHEMA IF EXISTS monitoring CASCADE;"'
make restore FILE=backup_YYYYMMDD_HHMM.dump
```

## Full Reset + Restore

Nuclear option — destroys all PostgreSQL data (`pgdata` volume), Kafka data (`kafka_data` volume), and Spark checkpoints before restoring from backup:
```bash
make reset-restore FILE=backup_YYYYMMDD_HHMM.dump
```

## What Happens After Restore

- PostgreSQL data is restored to the selected backup state
- Kafka data is NOT restored (separate volume)
- Spark checkpoints determine replay behavior:
  - If checkpoints exist — Spark continues from last processed offsets
  - If checkpoints are removed — full reprocessing from Kafka

If Kafka still contains newer events than the restored PostgreSQL state, Spark may reprocess them after restart. This may result in duplicate processing attempts (handled safely via idempotent writes) or temporary data gaps depending on checkpoint state. If Kafka retention is shorter than the backup age, some historical data may be permanently lost and cannot be reprocessed.

## If System Still Fails After Restore

1. Check service health:
   ```bash
   make health
   ```

2. Verify data is present and current:
   ```sql
   SELECT count(*) AS total_rows, max(event_ts) FROM public.raw_prices;
   ```

3. Check Spark logs:
   ```bash
   docker compose logs -f spark-stream
   ```

4. If needed, reset checkpoints:
   ```bash
   docker volume rm streaming_system_spark_checkpoints
   make real
   ```

5. Verify Kafka topic availability:
   ```bash
   docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic commodity_prices
   ```

## Backup Integrity

Backups are not automatically verified.

To validate backup structure:
```bash
pg_restore -l backup_YYYYMMDD_HHMM.dump
```

To perform a full validation, restore into a temporary database inside the PostgreSQL container:
```bash
docker compose exec postgres createdb -U "$POSTGRES_USER" test_restore
docker compose exec postgres pg_restore -U "$POSTGRES_USER" -d test_restore /backups/backup_YYYYMMDD_HHMM.dump
```
If restore fails, the backup may be corrupted or inconsistent.

## Volume Management

| Volume | Data | Recreatable? |
|--------|------|-------------|
| pgdata | All database data | **NO** — back up before `make downv` |
| kafka_data | Kafka logs/offsets | Yes (Spark replays from checkpoint) |
| spark_checkpoints | Spark offset state | Partially (earliest restarts from beginning) |
| spark_ivy_cache | Maven dependency cache | Yes (re-downloaded on start) |
| grafana_data | Grafana state | Yes (provisioned from config files) |
| ./backups | pg_dump archives | **NO** — stored on host filesystem |
