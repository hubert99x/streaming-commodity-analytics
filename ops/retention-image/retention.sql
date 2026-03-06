-- remove old raw price data

DELETE FROM raw_prices
WHERE event_ts < now() - interval '90 days';

-- clean DLQ history

DELETE FROM monitoring.dead_letter_events
WHERE ts_utc < now() - interval '90 days';

-- clean alert history

DELETE FROM monitoring.alert_events
WHERE ts_utc < now() - interval '90 days';

VACUUM ANALYZE;
