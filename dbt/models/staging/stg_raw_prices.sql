-- Staging layer for raw commodity prices.
-- Materialized as a view (no data copy) — acts as a clean interface between
-- the raw Spark sink and downstream mart models.
--
-- Explicit casts ensure consistent types even if source columns change.
--
-- Timestamps are passed through as timestamptz, exactly as public.raw_prices
-- stores them. Converting them to naive UTC here would make every downstream
-- comparison against now() depend on the session timezone, and the marts would
-- silently return wrong rows for any session that is not running in UTC.

select
  event_id::text as event_id,
  commodity::text as commodity,
  symbol::text as symbol,
  price::double precision as price,
  currency::text as currency,
  event_ts::timestamptz as event_ts,
  source::text as source,
  ingest_ts::timestamptz as ingest_ts,
  -- Kafka metadata retained for debugging (trace bad mart records back to source)
  kafka_partition::integer as kafka_partition,
  kafka_offset::bigint as kafka_offset
from {{ source('public', 'raw_prices') }}