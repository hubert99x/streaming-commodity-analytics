-- Most recent price per commodity (PostgreSQL DISTINCT ON picks the first row per group)
-- Only scans the last 24 hours to avoid full table scan as raw_prices grows.
-- Falls back to full scan for commodities missing from the 24h window.

with recent as (

    select distinct on (commodity, symbol)
      commodity,
      symbol,
      event_ts as last_timestamp,
      price as last_price,
      currency,
      source
    from {{ ref('stg_raw_prices') }}
    where event_ts >= now() - interval '24 hours'
    order by
      commodity,
      symbol,
      event_ts desc,
      event_id desc

),

fallback as (

    -- Commodities missing from the 24h window (e.g. extended downtime)
    select distinct on (s.commodity, s.symbol)
      s.commodity,
      s.symbol,
      s.event_ts as last_timestamp,
      s.price as last_price,
      s.currency,
      s.source
    from {{ ref('stg_raw_prices') }} s
    left join recent r on s.commodity = r.commodity and s.symbol = r.symbol
    where r.commodity is null
    order by
      s.commodity,
      s.symbol,
      s.event_ts desc,
      s.event_id desc

)

select * from recent
union all
select * from fallback
