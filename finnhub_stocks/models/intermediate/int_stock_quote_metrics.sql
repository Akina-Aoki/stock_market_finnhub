/*
Intermediate model adds stock-specific metrics on top of the clean staging model.

Input:
STAGING.STG_STOCK_QUOTES

Output:
INTERMEDIATE.INT_STOCK_QUOTE_METRICS

* Extracts fetched_date and fetched_hour from fetched_at.
* Extracts loaded_date and loaded_hour from loaded_at.
* Calculates daily_price_range as high_price - low_price.
* Calculates daily_price_range_percent.
* Adds price_movement_direction with values: up, down, unchanged.
* Calculates where the current price sits inside the daily high-low range.
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
    -- Example: 2026-06-29
    CAST(fetched_at AS DATE) AS fetched_date,

    -- Hour when the quote was fetched.
    -- Example: 06:00
    TO_CHAR(DATE_TRUNC('hour', fetched_at), 'HH24:MI') AS fetched_hour,


    -- Date when the row was loaded into Snowflake.
    -- Example: 2026-06-29
    CAST(loaded_at AS DATE) AS loaded_date,

    -- Hour when the row was loaded into Snowflake.
    -- Example: 03:00
    TO_CHAR(DATE_TRUNC('hour', loaded_at), 'HH24:MI') AS loaded_hour,


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