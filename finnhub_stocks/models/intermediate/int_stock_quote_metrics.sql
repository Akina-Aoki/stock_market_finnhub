/*
Intermediate model adds stock-specific metrics on top of the clean staging model.

Input:
STAGING.STG_STOCK_QUOTES

Output:
INTERMEDIATE.INT_STOCK_QUOTE_METRICS

* Extract `fetched_date` and `fetched_hour`.
* Calculate `daily_price_range` (High minus Low).
* Flag price movement (`up`, `down`, `flat`).
* Add simple QA flags (e.g., `is_current_price_missing`).
*/


WITH stock_quotes AS (

    SELECT
        symbol,
        current_price,
        price_change,
        price_change_percent,
        high_price,
        low_price,
        open_price,
        previous_close_price,
        finnhub_timestamp,
        fetched_at,
        source_file_name,
        loaded_at
    FROM {{ ref('stg_stock_quotes') }}

)

SELECT
    symbol,
    current_price,
    price_change,
    price_change_percent,
    high_price,
    low_price,
    open_price,
    previous_close_price,
    finnhub_timestamp,
    fetched_at,
    source_file_name,
    loaded_at,

    -- Date when the quote was fetched.
    CAST(fetched_at AS DATE) AS fetched_date,

    -- Hour when the quote was fetched.
    -- Useful for hourly analysis.
    DATE_TRUNC('hour', fetched_at) AS fetched_hour,

    -- Difference between the daily high and daily low price
    high_price - low_price AS daily_price_range,

    -- Daily price range as a percentage of the previous close.
    -- NULLIF prevents division by zero.
    ((high_price - low_price) / NULLIF(previous_close_price, 0)) * 100 AS daily_price_range_percent,

    -- Simple movement label.
    CASE
        WHEN price_change > 0 THEN 'up'
        WHEN price_change < 0 THEN 'down'
        ELSE 'unchanged'
    END AS price_movement_direction,

    -- Shows where the current price sits inside the daily range.
    -- 0 means near the low, 1 means near the high.
    (current_price - low_price) / NULLIF(high_price - low_price, 0) AS price_position_in_daily_range

FROM stock_quotes