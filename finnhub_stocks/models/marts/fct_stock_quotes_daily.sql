final AS (

    SELECT
        -- Unique ID for each daily stock quote row.
        -- Example: AAPL0001_20260629
        CONCAT(s.stock_symbol_id, '_', d.date_id) AS stock_quote_id,

        -- Foreign keys to the dimension tables.
        s.stock_symbol_id,
        d.date_id,

        -- Stock price measures.
        ROUND(q.current_price, 2) AS current_price,
        ROUND(q.price_change, 2) AS price_change,
        ROUND(q.price_change_percent, 2) AS price_change_percent,
        ROUND(q.high_price, 2) AS high_price,
        ROUND(q.low_price, 2) AS low_price,
        ROUND(q.open_price, 2) AS open_price,
        ROUND(q.previous_close_price, 2) AS previous_close_price,

        -- Calculated stock metrics.
        ROUND(q.daily_price_range, 2) AS daily_price_range,
        ROUND(q.daily_price_range_percent, 2) AS daily_price_range_percent,
        q.price_movement_direction,
        ROUND(q.price_position_in_daily_range, 4) AS price_position_in_daily_range

    FROM latest_daily_quotes q

    LEFT JOIN {{ ref('dim_stock_symbol') }} s
        ON q.symbol = s.symbol

    LEFT JOIN {{ ref('dim_date') }} d
        ON q.fetched_date = d.calendar_date

)