/*
Daily fact table for stock quotes.

Grain:
One row per stock symbol per fetched date.

Main rule:
If we collected the same stock more than once on the same day,
we keep the latest quote based on fetched_at.
*/


WITH ranked_quotes AS (

    SELECT
        symbol,
        fetched_date,
        current_price,
        price_change,
        price_change_percent,
        high_price,
        low_price,
        open_price,
        previous_close_price,
        daily_price_range,
        daily_price_range_percent,
        price_movement_direction,
        price_position_in_daily_range,
        fetched_at,

        -- Give each quote a rank inside each symbol/date group.
        -- The latest fetched_at gets row_number = 1.
        ROW_NUMBER() OVER (
            PARTITION BY symbol, fetched_date
            ORDER BY fetched_at DESC
        ) AS row_number

    FROM {{ ref('int_stock_quote_metrics') }}

),

latest_daily_quotes AS (

    SELECT
        symbol,
        fetched_date,
        current_price,
        price_change,
        price_change_percent,
        high_price,
        low_price,
        open_price,
        previous_close_price,
        daily_price_range,
        daily_price_range_percent,
        price_movement_direction,
        price_position_in_daily_range

    FROM ranked_quotes

    -- Keep only the latest quote per symbol per date.
    WHERE row_number = 1

),

final AS (

    SELECT
        -- Unique ID for each daily stock quote row.
        -- Example: AAPL_20260629
        CONCAT(q.symbol, '_', d.date_id) AS stock_quote_id,

        -- Foreign keys that connect this fact table to dimensions.
        s.stock_symbol_id,
        d.date_id,

        -- Readable business columns.
        q.symbol,
        q.fetched_date,

        -- Rounded values for the final analytics-ready table.
        ROUND(q.current_price, 2) AS current_price,
        ROUND(q.price_change, 2) AS price_change,
        ROUND(q.price_change_percent, 2) AS price_change_percent,
        ROUND(q.high_price, 2) AS high_price,
        ROUND(q.low_price, 2) AS low_price,
        ROUND(q.open_price, 2) AS open_price,
        ROUND(q.previous_close_price, 2) AS previous_close_price,

        -- Rounded calculated metrics.
        ROUND(q.daily_price_range, 2) AS daily_price_range,
        ROUND(q.daily_price_range_percent, 2) AS daily_price_range_percent,
        q.price_movement_direction,
        ROUND(q.price_position_in_daily_range, 4) AS price_position_in_daily_range

    FROM latest_daily_quotes q

    -- Join to stock symbol dimension to get stock_symbol_id.
    LEFT JOIN {{ ref('dim_stock_symbol') }} s
        ON q.symbol = s.symbol

    -- Join to date dimension to get date_id.
    LEFT JOIN {{ ref('dim_date') }} d
        ON q.fetched_date = d.calendar_date

)

SELECT *
FROM final