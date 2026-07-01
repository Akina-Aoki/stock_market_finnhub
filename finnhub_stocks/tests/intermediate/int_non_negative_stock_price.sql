/*
dbt test checks that stock price fields are not negative.
If this query returns any rows, the test fails.
In normal stock quote data, prices should be zero or positive.
*/

SELECT
    symbol,
    current_price,
    high_price,
    low_price,
    open_price,
    previous_close_price,
    fetched_at
FROM {{ ref('stg_stock_quotes') }}
WHERE
    current_price < 0
    OR high_price < 0
    OR low_price < 0
    OR open_price < 0
    OR previous_close_price < 0