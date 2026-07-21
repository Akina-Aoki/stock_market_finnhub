-- 1. Create project database
CREATE DATABASE IF NOT EXISTS finnhub_stocks_mds;


-- 2. Create cost-controlled warehouse
CREATE WAREHOUSE IF NOT EXISTS finnhub_stocks_mds_wh
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;


-- 3. Use project warehouse and database
USE WAREHOUSE finnhub_stocks_mds_wh;
USE DATABASE finnhub_stocks_mds;



-- 4. Create schemas using fully qualified names

-- Store JSONL records loaded from S3 with minimal/no transformation
CREATE SCHEMA IF NOT EXISTS finnhub_stocks_mds.raw;

-- Flatten JSON fields, rename columns, cast data types, etc.
CREATE SCHEMA IF NOT EXISTS finnhub_stocks_mds.staging;

-- Add business logic
CREATE SCHEMA IF NOT EXISTS finnhub_stocks_mds.intermediate;

-- Create analytics-friendly dashboard tables
CREATE SCHEMA IF NOT EXISTS finnhub_stocks_mds.marts;


-- 5. Use raw schema for the next step
USE SCHEMA finnhub_stocks_mds.raw;


-- 6. Validation
SELECT 
    CURRENT_DATABASE() AS current_database,
    CURRENT_SCHEMA() AS current_schema,
    CURRENT_WAREHOUSE() AS current_warehouse;
