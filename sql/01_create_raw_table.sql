-- Create the raw Snowflake table and JSONL file format
-- for stock quote records loaded from Amazon S3.

-- 1. Use correct Snowflake context
USE WAREHOUSE finnhub_stocks_mds_wh;
USE DATABASE finnhub_stocks_mds;
USE SCHEMA raw;


-- 2. Create raw table for JSONL stock quote records
CREATE TABLE IF NOT EXISTS raw_stock_quotes (
    raw_record VARIANT,
    source_file_name STRING,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);



-- 3. Create JSON file format for JSONL files
CREATE FILE FORMAT IF NOT EXISTS jsonl_file_format
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE;



-- 4. Validate objects
SHOW TABLES IN SCHEMA finnhub_stocks_mds.raw;

SHOW FILE FORMATS IN SCHEMA finnhub_stocks_mds.raw;