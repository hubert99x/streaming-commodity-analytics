# Disaster Recovery

## Backup Schedule

Automated backups run every 2 hours via `backup-cron` service (`ops` profile). Each backup is a `pg_dump -F c` archive stored in `./backups/` on the host. Last 360 backups are retained (~30 days).

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

If restore fails with "cannot drop schema" errors, drop schemas first:
```bash
docker compose exec postgres sh -lc \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
   DROP SCHEMA IF EXISTS analytics CASCADE;
   DROP SCHEMA IF EXISTS monitoring CASCADE;"'
make restore FILE=backup_YYYYMMDD_HHMM.dump
```

## Full Reset + Restore

Nuclear option — destroys all data and restores from a backup:
```bash
make reset-restore FILE=backup_YYYYMMDD_HHMM.dump
```

## Volume Management

| Volume | Data | Recreatable? |
|--------|------|-------------|
| pgdata | All database data | **NO** — back up before `make downv` |
| kafka_data | Kafka logs/offsets | Yes (Spark replays from checkpoint) |
| spark_checkpoints | Spark offset state | Partially (earliest restarts from beginning) |
| spark_ivy_cache | Maven dependency cache | Yes (re-downloaded on start) |
| grafana_data | Grafana state | Yes (provisioned from config files) |
| ./backups | pg_dump archives | **NO** — stored on host filesystem |
