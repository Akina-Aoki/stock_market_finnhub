-- Create a secure Snowflake storage integration for reading
-- raw Finnhub stock quote JSONL files from Amazon S3.

USE ROLE ACCOUNTADMIN;


CREATE STORAGE INTEGRATION IF NOT EXISTS finnhub_stocks_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::906698969534:role/snowflake_finnhub_stocks_s3_read_role'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://finnhub-stocks/raw/stock_quotes/');



DESC INTEGRATION finnhub_stocks_s3_integration;


-- Create an external stage that points Snowflake to the raw JSONL files in Amazon S3.

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE finnhub_stocks_mds_wh;
USE DATABASE finnhub_stocks_mds;
USE SCHEMA raw;

CREATE STAGE IF NOT EXISTS finnhub_stock_quotes_stage
    URL = 's3://finnhub-stocks/raw/stock_quotes/'
    STORAGE_INTEGRATION = finnhub_stocks_s3_integration
    FILE_FORMAT = jsonl_file_format;



-- test if Snowflake can see S3 file
LIST @finnhub_stock_quotes_stage;



-- Validation Query
SELECT
    CURRENT_ROLE() AS current_role,
    CURRENT_WAREHOUSE() AS current_warehouse,
    CURRENT_DATABASE() AS current_database,
    CURRENT_SCHEMA() AS current_schema;




-- test load the JSONL file into the raw table
COPY INTO raw_stock_quotes (raw_record, source_file_name)
FROM (
    SELECT
        $1 AS raw_record,
        METADATA$FILENAME AS source_file_name
    FROM @finnhub_stock_quotes_stage
)
FILE_FORMAT = (FORMAT_NAME = jsonl_file_format)
ON_ERROR = 'CONTINUE';




-- Check the table

SELECT *
FROM raw_stock_quotes
LIMIT 5;


SELECT COUNT(*) AS total_rows
FROM raw_stock_quotes;




-- extract JSON fields
SELECT
    raw_record:symbol::STRING AS symbol,
    raw_record:current_price::FLOAT AS current_price,
    raw_record:change::FLOAT AS price_change,
    raw_record:percent_change::FLOAT AS percent_change,
    raw_record:high_price::FLOAT AS high_price,
    raw_record:low_price::FLOAT AS low_price,
    raw_record:open_price::FLOAT AS open_price,
    raw_record:previous_close_price::FLOAT AS previous_close_price,
    raw_record:finnhub_timestamp::NUMBER AS finnhub_timestamp,
    raw_record:fetched_at::TIMESTAMP_NTZ AS fetched_at,
    source_file_name,
    loaded_at
FROM raw_stock_quotes
LIMIT 10;