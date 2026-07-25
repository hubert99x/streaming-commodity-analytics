-- Retention: delete records older than 90 days from all data and monitoring tables.
-- Shared by: dbt-scheduler (every 24h) and retention daemon (ops profile, every 24h).
--
-- DELETE only. VACUUM lives in ops/sql/vacuum.sql because it requires table
-- ownership: run as dbt_runner it emits "permission denied to vacuum, skipping"
-- and still exits 0, so ON_ERROR_STOP never catches the silent no-op.

DELETE FROM public.raw_prices
WHERE event_ts < now() - interval '90 days';

DELETE FROM monitoring.dead_letter_events
WHERE ts_utc < now() - interval '90 days';

DELETE FROM monitoring.alert_events
WHERE ts_utc < now() - interval '90 days';

DELETE FROM monitoring.api_calls
WHERE ts_utc < now() - interval '90 days';

DELETE FROM monitoring.kafka_lag
WHERE ts_utc < now() - interval '90 days';

DELETE FROM monitoring.dbt_test_runs
WHERE run_ts_utc < now() - interval '90 days';

DELETE FROM monitoring.backup_log
WHERE backup_ts < now() - interval '90 days';
