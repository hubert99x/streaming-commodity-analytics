-- Explicit casts ensure consistent types even if source columns change.
-- "at time zone 'utc'" converts timestamptz to naive timestamp in UTC for downstream joins.

select
  event_id::text as event_id,
  commodity::text as commodity,
  symbol::text as symbol,
  price::double precision as price,
  currency::text as currency,
  event_ts at time zone 'utc' as event_ts,
  source::text as source,
  ingest_ts at time zone 'utc' as ingest_ts,
  -- Kafka metadata retained for debugging (trace bad mart records back to source)
  kafka_partition::integer as kafka_partition,
  kafka_offset::bigint as kafka_offset
from public.raw_prices