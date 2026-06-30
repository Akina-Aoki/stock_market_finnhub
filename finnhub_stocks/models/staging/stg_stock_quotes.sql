/*
This is the first dbt staging model for Finnhub stock quote data.
1. Read raw JSON records from the RAW layer.
2. Extract JSON fields into normal SQL columns.
3. Standardize column names and data types.

Input table:
FINNHUB_STOCKS_MDS.RAW.RAW_STOCK_QUOTES

Output model:
FINNHUB_STOCKS_MDS.STAGING.STG_STOCK_QUOTES
*/



/*
    This CTE reads from the raw Snowflake table.
    In dbt, {{ source('raw', 'RAW_STOCK_QUOTES') }}
    refers to the source table defined in models/staging/sources.yml
*/

WITH source AS (

    SELECT
        raw_record,          -- VARIANT column containing the full raw JSON object from Finnhub
        source_file_name,    -- Name/path of the S3 file where the record came from
        loaded_at            -- Timestamp when the record was loaded into Snowflake
    FROM {{ source('raw', 'RAW_STOCK_QUOTES') }}

)

SELECT
    -- Extract the stock ticker symbol from the JSON. (AAPL, MSFT, TSLA, etc)
    raw_record:symbol::STRING AS symbol,

    -- Current stock price from Finnhub.
    raw_record:current_price::FLOAT AS current_price,

    -- Price change compared to previous close.
    raw_record:change::FLOAT AS price_change,

    -- Percentage change compared to previous close.
    raw_record:percent_change::FLOAT AS price_change_percent,

    -- Highest price of the trading day.
    raw_record:high_price::FLOAT AS high_price,

    -- Lowest price of the trading day.
    raw_record:low_price::FLOAT AS low_price,

    -- Opening price of the trading day.
    raw_record:open_price::FLOAT AS open_price,

    -- Previous closing price.
    raw_record:previous_close_price::FLOAT AS previous_close_price,

    -- Finnhub timestamp from the API.
    -- This is currently kept as a number.
    -- Later, we can convert this into a timestamp
    raw_record:finnhub_timestamp::NUMBER AS finnhub_timestamp,

    -- Timestamp from our Python producer showing when the API request was made.
    raw_record:fetched_at::TIMESTAMP_NTZ AS fetched_at,

    -- Metadata columns from the raw loading process.
    -- These help us trace where each row came from.
    source_file_name,
    loaded_at

FROM source