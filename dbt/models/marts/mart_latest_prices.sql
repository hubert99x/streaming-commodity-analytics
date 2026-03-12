-- Most recent price per commodity (PostgreSQL DISTINCT ON picks the first row per group)

select distinct on (commodity, symbol)
  commodity,
  symbol,
  event_ts as last_timestamp,
  price as last_price,
  currency,
  source
from {{ ref('stg_raw_prices') }}
order by
  commodity,
  symbol,
  event_ts desc,
  event_id desc