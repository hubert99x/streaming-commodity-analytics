{{
  config(
    post_hook="CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_event_ts ON {{ this }} (event_ts DESC)"
  )
}}
-- Detect significant price changes between consecutive observations

with base as (

    select
        commodity,
        symbol,
        event_ts,
        price,
        lag(price) over (
            partition by commodity, symbol
            order by event_ts
        ) as prev_price,
        lag(event_ts) over (
            partition by commodity, symbol
            order by event_ts
        ) as prev_event_ts
    from {{ ref('stg_raw_prices') }}

),

changes as (

    select
        commodity,
        symbol,
        event_ts,
        prev_event_ts,
        price,
        prev_price,
        case
            when prev_price is not null and prev_price <> 0
                then (price - prev_price) / prev_price
            else null
        end as pct_change
    from base

),

classified as (

    select
        commodity,
        symbol,
        event_ts,
        prev_event_ts,
        price,
        prev_price,
        pct_change,
        -- Thresholds reflect each asset's typical volatility:
        -- BTC (~1.5% daily) > XAU (~0.6%) > EUR/USD (~0.25%)
        case
            -- BTC: high volatility - 1.5% / 0.7% / 0.3%
            when symbol = 'BTC/USD' and abs(pct_change) >= 0.015 then 'EXTREME_MOVE'
            when symbol = 'BTC/USD' and abs(pct_change) >= 0.007 then 'LARGE_MOVE'
            when symbol = 'BTC/USD' and abs(pct_change) >= 0.003 then 'MEDIUM_MOVE'

            -- GOLD: medium volatility - 0.6% / 0.3% / 0.15%
            when symbol = 'XAU/USD' and abs(pct_change) >= 0.006 then 'EXTREME_MOVE'
            when symbol = 'XAU/USD' and abs(pct_change) >= 0.003 then 'LARGE_MOVE'
            when symbol = 'XAU/USD' and abs(pct_change) >= 0.0015 then 'MEDIUM_MOVE'

            -- EURUSD: low volatility - 0.25% / 0.12% / 0.06%
            when symbol = 'EUR/USD' and abs(pct_change) >= 0.0025 then 'EXTREME_MOVE'
            when symbol = 'EUR/USD' and abs(pct_change) >= 0.0012 then 'LARGE_MOVE'
            when symbol = 'EUR/USD' and abs(pct_change) >= 0.0006 then 'MEDIUM_MOVE'

            else 'NORMAL'
        end as event_type
    from changes
    -- Filter out first observation per commodity (no previous price to compare)
    -- Filter out observations after long gaps (e.g. FX weekend) to avoid false move alerts
    where prev_price is not null
      and event_ts - prev_event_ts < interval '30 minutes'

)

select
    commodity,
    symbol,
    prev_event_ts,
    event_ts,
    extract(epoch from (event_ts - prev_event_ts))::integer as time_gap_seconds,
    price as current_price,
    prev_price,
    round((pct_change * 100)::numeric, 4) as price_change_pct,
    event_type
from classified
where event_type <> 'NORMAL'
order by event_ts desc, symbol
