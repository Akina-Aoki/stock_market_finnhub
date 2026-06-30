/*
dbt test checks that the daily high price is greater than or equal to the daily low price.
If this query returns any rows, the test fails.
*/

SELECT
    symbol,
    high_price,
    low_price,
    fetched_at
FROM {{ ref('stg_stock_quotes') }}
WHERE high_price < low_price