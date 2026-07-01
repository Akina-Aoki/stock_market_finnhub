-- This test fails if the daily high price is lower than the daily low price.

SELECT *
FROM {{ ref('fct_stock_quotes_daily') }}
WHERE high_price < low_price