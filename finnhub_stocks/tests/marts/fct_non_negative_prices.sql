-- This test fails if any price-related value is negative.
-- Note: price_change and price_change_percent are excluded because they can be negative.

SELECT *
FROM {{ ref('fct_stock_quotes_daily') }}
WHERE current_price < 0
   OR high_price < 0
   OR low_price < 0
   OR open_price < 0
   OR previous_close_price < 0
   OR daily_price_range < 0
   OR daily_price_range_percent < 0