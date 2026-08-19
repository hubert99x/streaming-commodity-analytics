{{
  config(
    materialized='incremental',
    unique_key=['commodity', 'symbol', 'event_ts'],
    on_schema_change='sync_all_columns',
    post_hook="CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_event_ts ON {{ this }} (event_ts DESC)"
  )
}}
-- Detect significant price changes between consecutive observations.
-- Used by the Grafana "Recent Price Events" table (Market Analysis dashboard).
--
-- Classifies each price change as MEDIUM_MOVE / LARGE_MOVE / EXTREME_MOVE
-- using per-commodity thresholds that reflect each asset's typical volatility.
-- NORMAL changes are filtered out (WHERE event_type <> 'NORMAL') to keep
-- the table focused on actionable price movements.
--
-- Incremental: looks back 2 hours so the LAG() window function can see
-- the row before the incremental boundary, avoiding false "first observation" gaps.

with base as (

    select
        commodity,
        symbol,
        event_ts,
        price,
        lag(price) over (
            partition by commodity, symbol
            order by event_ts, event_id
        ) as prev_price,
        lag(event_ts) over (
            partition by commodity, symbol
            order by event_ts, event_id
        ) as prev_event_ts
    from {{ ref('stg_raw_prices') }}
    {% if is_incremental() %}
    -- Lookback 2 hours so LAG() can see the row before the incremental boundary.
    -- coalesce guards the empty-table case: this model only stores non-NORMAL
    -- events, so a full refresh during a quiet period leaves it empty, max()
    -- returns NULL and the predicate would reject every row from then on.
    where event_ts >= coalesce(
        (select max(event_ts) from {{ this }}) - interval '2 hours',
        now() - interval '7 days'
    )
    {% endif %}

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

-- No ORDER BY here: row order inside a table is not guaranteed, and every
-- Grafana panel reading this model sorts for itself.
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
