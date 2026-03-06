-- Fail if any instrument jumps "too much" between consecutive observed buckets
-- This is a data-quality guardrail (likely bad tick / parsing / API glitch), not a trading rule.

with m as (
  select
    commodity,
    symbol,
    minute_bucket,
    last_price,
    lag(last_price) over (
      partition by commodity, symbol
      order by minute_bucket
    ) as prev_price
  from {{ ref('mart_minute_last_price') }}
),
x as (
  select
    *,
    case
      when prev_price is null or prev_price = 0 then null
      else abs(last_price - prev_price) / prev_price
    end as pct_change
  from m
)
select *
from x
where pct_change is not null
  and (
       (symbol = 'EUR/USD' and pct_change > 0.05)
    or (symbol = 'XAU/USD' and pct_change > 0.10)
    or (symbol = 'BTC/USD' and pct_change > 0.30)
  )
