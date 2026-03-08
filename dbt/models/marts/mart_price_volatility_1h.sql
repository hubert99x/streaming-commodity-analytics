-- Hourly volatility metrics based on stream observations

with base as (

    select
        commodity,
        symbol,
        date_trunc('hour', event_ts) as hour_bucket,
        price
    from {{ ref('stg_raw_prices') }}

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
order by hour_bucket desc, symbol
