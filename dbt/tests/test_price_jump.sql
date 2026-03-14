-- Data quality test: fail if any instrument's price jumps beyond sanity thresholds
-- between consecutive minute buckets.
--
-- Thresholds are intentionally wide (EUR 5%, XAU 10%, BTC 30%) — these catch
-- clearly erroneous data (API glitches, parsing bugs), not normal volatility.
-- If this test fails, check the DLQ and raw_prices for bad ticks.

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
