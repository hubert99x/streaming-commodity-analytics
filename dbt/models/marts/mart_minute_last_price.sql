{{
  config(
    materialized='incremental',
    unique_key=['commodity', 'symbol', 'minute_bucket'],
    on_schema_change='sync_all_columns',
    post_hook="CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_bucket ON {{ this }} (minute_bucket DESC)"
  )
}}
-- Last observed price per minute bucket (most recent wins via ordered array_agg)

select
  commodity,
  symbol,
  (date_trunc('minute', event_ts) AT TIME ZONE 'UTC') as minute_bucket,

  -- Pick the latest price within the minute; event_id breaks ties
  (array_agg(price order by event_ts desc, event_id desc))[1] as last_price,

  count(*) as n,
  min(price) as min_price,
  max(price) as max_price

from {{ ref('stg_raw_prices') }}
{% if is_incremental() %}
-- Only recompute last 30 minutes (handles late-arriving data)
where date_trunc('minute', event_ts) >= (select max(minute_bucket) - interval '30 minutes' from {{ this }})
{% endif %}
group by 1,2,3
