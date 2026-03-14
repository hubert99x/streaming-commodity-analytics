-- remove old raw price data

DELETE FROM raw_prices
WHERE event_ts < now() - interval '90 days';

-- clean DLQ history

DELETE FROM monitoring.dead_letter_events
WHERE ts_utc < now() - interval '90 days';

-- clean alert history

DELETE FROM monitoring.alert_events
WHERE ts_utc < now() - interval '90 days';

-- clean API call logs

DELETE FROM monitoring.api_calls
WHERE ts_utc < now() - interval '90 days';

-- clean Kafka lag history

DELETE FROM monitoring.kafka_lag
WHERE ts_utc < now() - interval '90 days';

-- clean dbt test run history

DELETE FROM monitoring.dbt_test_runs
WHERE ts_utc < now() - interval '90 days';

-- clean backup log history

DELETE FROM monitoring.backup_log
WHERE ts_utc < now() - interval '90 days';

VACUUM ANALYZE;
