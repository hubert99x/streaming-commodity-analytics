{{
  config(
    materialized='incremental',
    unique_key=['commodity', 'symbol', 'minute_bucket'],
    on_schema_change='sync_all_columns',
    post_hook="CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_bucket ON {{ this }} (minute_bucket DESC)"
  )
}}
-- Minute-level price aggregation: last observed price per minute bucket.
-- Used as the base for the test_price_jump data quality test.
--
-- Incremental: on each run, only recomputes the last 30 minutes to handle
-- late-arriving data without reprocessing the entire history.
-- The ordered array_agg picks the latest price within each minute (event_id breaks ties).

select
  commodity,
  symbol,
  -- stg_raw_prices already exposes event_ts as a naive UTC timestamp, so no
  -- further conversion here: an extra AT TIME ZONE would turn minute_bucket
  -- back into timestamptz and make it the only mart column of that type.
  date_trunc('minute', event_ts) as minute_bucket,

  -- Pick the latest price within the minute; event_id breaks ties
  (array_agg(price order by event_ts desc, event_id desc))[1] as last_price,

  count(*) as n,
  min(price) as min_price,
  max(price) as max_price

from {{ ref('stg_raw_prices') }}
{% if is_incremental() %}
-- Only recompute last 30 minutes (handles late-arriving data).
-- coalesce keeps an empty table from producing a NULL bound that matches nothing.
where date_trunc('minute', event_ts) >= coalesce(
    (select max(minute_bucket) from {{ this }}) - interval '30 minutes',
    now() - interval '7 days'
)
{% endif %}
group by 1,2,3
