-- This test fails if price_position_in_daily_range is outside the expected range.
-- 0 means near the daily low, 1 means near the daily high.

SELECT *
FROM {{ ref('fct_stock_quotes_daily') }}
WHERE price_position_in_daily_range < 0
   OR price_position_in_daily_range > 1