-- Performance indexes for raw_prices.
-- Run once as superuser or table owner.

-- Covers "latest price per commodity" queries (Grafana panels, mart_latest_prices pattern)
CREATE INDEX IF NOT EXISTS idx_raw_prices_commodity_event_ts
ON public.raw_prices (commodity, event_ts DESC, event_id DESC);

-- Covers "seconds since last ingest" and time-range filters
CREATE INDEX IF NOT EXISTS idx_raw_prices_event_ts
ON public.raw_prices (event_ts DESC);

-- Covers kafka lag monitor query (MAX(kafka_offset) per partition)
CREATE INDEX IF NOT EXISTS idx_raw_prices_partition_offset
ON public.raw_prices (kafka_partition, kafka_offset DESC);

-- DLQ idempotent upsert constraint (prevents duplicate DLQ entries on batch replay)
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_dlq_event'
    ) THEN
        ALTER TABLE monitoring.dead_letter_events
        ADD CONSTRAINT uq_dlq_event
        UNIQUE (stream_instance_id, batch_id, kafka_partition, kafka_offset);
    END IF;
END $$;
