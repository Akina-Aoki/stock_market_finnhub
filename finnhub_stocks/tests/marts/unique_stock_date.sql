-- This test fails if there is more than one row for the same stock symbol and date.
-- This protects the fact table grain: one row per stock symbol per fetched date.

SELECT
    stock_symbol_id,
    date_id,
    COUNT(*) AS row_count
FROM {{ ref('fct_stock_quotes_daily') }}
GROUP BY
    stock_symbol_id,
    date_id
HAVING COUNT(*) > 1