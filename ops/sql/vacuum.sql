-- Reclaim disk space and refresh planner statistics after retention deletes.
--
-- Must run as the table owner (postgres), so only the retention daemon in the
-- ops profile executes it — the dbt-scheduler connects as dbt_runner and would
-- silently skip every table.
--
-- Run weekly rather than daily: VACUUM takes an exclusive-ish lock per table
-- and the daily delete volume on a 90-day window is small.

VACUUM (ANALYZE) public.raw_prices;
VACUUM (ANALYZE) monitoring.dead_letter_events;
VACUUM (ANALYZE) monitoring.alert_events;
VACUUM (ANALYZE) monitoring.api_calls;
VACUUM (ANALYZE) monitoring.kafka_lag;
VACUUM (ANALYZE) monitoring.dbt_test_runs;
VACUUM (ANALYZE) monitoring.backup_log;
