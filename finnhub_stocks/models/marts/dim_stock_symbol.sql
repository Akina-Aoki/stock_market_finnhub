/*
Dimension table for stock symbols.

Input:
INTERMEDIATE.INT_STOCK_QUOTE_METRICS

Output:
MARTS.DIM_STOCK_SYMBOL

This model creates one row per stock symbol.
It will be used as the stock/company dimension in the star schema.

Grain:
One row per stock symbol.
*/


WITH symbols AS (

    SELECT DISTINCT
        UPPER(symbol) AS symbol
    FROM {{ ref('int_stock_quote_metrics') }}

)

SELECT
    -- Readable generated ID for each stock symbol.
    -- Example: AAPL becomes AAPL0001.
    CONCAT(symbol, '0001') AS stock_symbol_id,

    -- Stock ticker symbol, such as AAPL, MSFT, TSLA, GOOGL, or AMZN.
    symbol

FROM symbols