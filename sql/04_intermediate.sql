-- view table
SELECT *
FROM FINNHUB_STOCKS_MDS.INTERMEDIATE.INT_STOCK_QUOTE_METRICS
LIMIT 10;

-- test
SELECT
    symbol,
    fetched_at,
    fetched_date,
    fetched_hour,
    fetched_hour_label,
    loaded_at,
    loaded_date,
    loaded_hour,
    loaded_hour_label
FROM FINNHUB_STOCKS_MDS.INTERMEDIATE.INT_STOCK_QUOTE_METRICS
LIMIT 10;