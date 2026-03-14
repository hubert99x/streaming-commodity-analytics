-- Retention: delete records older than 90 days from all data and monitoring tables.
-- Shared by: dbt-scheduler (automated, every 24h) and retention container (manual).
-- NOTE: Does not run VACUUM — callers with superuser privileges can VACUUM separately.

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
WHERE ts_utc < now() - interval '90 days';

DELETE FROM monitoring.backup_log
WHERE ts_utc < now() - interval '90 days';
