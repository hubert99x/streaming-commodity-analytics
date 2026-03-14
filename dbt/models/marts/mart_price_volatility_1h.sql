{{
  config(
    materialized='incremental',
    unique_key=['commodity', 'symbol', 'hour_bucket'],
    on_schema_change='sync_all_columns',
    post_hook="CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_bucket ON {{ this }} (hour_bucket DESC)"
  )
}}
-- Hourly volatility metrics: avg price, range, std dev per instrument per hour.
-- Used by Grafana "Price Statistics" table, "Hourly Range" chart, and "Market Summary" panel.
--
-- range_pct = (high - low) / avg * 100 — a normalized measure of price spread
-- that allows comparing volatility across instruments with different price levels
-- (e.g. BTC at $70k vs EUR/USD at $1.08).
--
-- Excludes the current incomplete hour to avoid artificially low volatility readings.
-- Incremental: recomputes the last 2 completed hours to handle late-arriving data.

with base as (

    select
        commodity,
        symbol,
        date_trunc('hour', event_ts) as hour_bucket,
        price
    from {{ ref('stg_raw_prices') }}
    {% if is_incremental() %}
    -- Only recompute the last 2 completed hours (handles late-arriving data)
    where date_trunc('hour', event_ts) >= (select max(hour_bucket) - interval '2 hours' from {{ this }})
    {% endif %}

),

aggregated as (

    select
        commodity,
        symbol,
        hour_bucket,
        count(*) as observations_count,
        avg(price) as avg_price,
        min(price) as min_price,
        max(price) as max_price,
        stddev_samp(price) as price_stddev,
        max(price) - min(price) as price_range,
        -- Normalized price range: (high - low) / avg - measures spread relative to price level
        case
            when avg(price) is not null and avg(price) <> 0
                then (max(price) - min(price)) / avg(price)
            else null
        end as range_pct
    from base
    group by 1, 2, 3

)

select
    commodity,
    symbol,
    hour_bucket,
    observations_count,
    round(avg_price::numeric, 6) as avg_price,
    round(min_price::numeric, 6) as min_price,
    round(max_price::numeric, 6) as max_price,
    round(coalesce(price_stddev, 0)::numeric, 6) as price_stddev,
    round(price_range::numeric, 6) as price_range,
    round((range_pct * 100)::numeric, 4) as range_pct
from aggregated
-- Exclude current incomplete hour to avoid artificially low volatility
where hour_bucket < date_trunc('hour', now())
order by hour_bucket desc, symbol
