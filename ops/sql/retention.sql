-- Retention: delete records older than 90 days from all data and monitoring tables.
-- Shared by: dbt-scheduler (every 24h) and retention daemon (ops profile, every 24h).
-- After each DELETE, VACUUM reclaims disk space and updates visibility maps
-- so that the freed pages are available for reuse.

DELETE FROM public.raw_prices
WHERE event_ts < now() - interval '90 days';
VACUUM public.raw_prices;

DELETE FROM monitoring.dead_letter_events
WHERE ts_utc < now() - interval '90 days';
VACUUM monitoring.dead_letter_events;

DELETE FROM monitoring.alert_events
WHERE ts_utc < now() - interval '90 days';
VACUUM monitoring.alert_events;

DELETE FROM monitoring.api_calls
WHERE ts_utc < now() - interval '90 days';
VACUUM monitoring.api_calls;

DELETE FROM monitoring.kafka_lag
WHERE ts_utc < now() - interval '90 days';
VACUUM monitoring.kafka_lag;

DELETE FROM monitoring.dbt_test_runs
WHERE run_ts_utc < now() - interval '90 days';
VACUUM monitoring.dbt_test_runs;

DELETE FROM monitoring.backup_log
WHERE backup_ts < now() - interval '90 days';
VACUUM monitoring.backup_log;
