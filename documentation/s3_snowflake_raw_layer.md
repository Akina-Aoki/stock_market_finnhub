# Amazon S3 → Snowflake Raw Layer Documentation

## 1. Overview

This document explains the second ingestion part of the Finnhub stock market data pipeline.

The previous phase completed this flow:

```text
Finnhub API
→ Python producer
→ Kafka topic: stock_quotes
→ Python consumer
→ Amazon S3 raw JSONL files
```

This phase continues from Amazon S3 into Snowflake:

```text
Amazon S3 JSONL files
→ Snowflake storage integration
→ Snowflake external stage
→ Snowflake raw table
→ JSON field extraction validation
```

The purpose of this phase is to load raw stock quote JSONL files from Amazon S3 into Snowflake, while keeping the raw data mostly unchanged.

This creates the foundation for the future dbt models:

```text
Snowflake raw table
→ dbt staging model: stg_stock_quotes
→ dbt intermediate models
→ dbt mart models
```

---

## 2. Why this phase matters

Amazon S3 is used as the raw file storage layer.

Snowflake is used as the cloud data warehouse.

The raw JSONL files are first loaded into a Snowflake raw table before any cleaning or business logic is applied.

This is important because the raw layer keeps a copy of the original ingested data.

The pipeline design is:

```text
S3 raw files
→ Snowflake raw table
→ dbt staging
→ dbt intermediate
→ dbt marts
```

In this project, the raw Snowflake table is not the same as the dbt staging layer.

The raw table stores the original JSON records.

The dbt staging layer will later flatten, rename, and cast the raw JSON fields.

---

## 3. Current architecture

Current completed flow:

```text
Finnhub API
→ Kafka producer
→ Kafka topic: stock_quotes
→ Kafka consumer
→ Amazon S3
→ Snowflake external stage
→ Snowflake raw table
```

More detailed version:

```text
Finnhub API
→ producer/producer.py
→ Kafka topic: stock_quotes
→ consumer/consumer.py
→ s3://finnhub-stocks/raw/stock_quotes/
→ FINNHUB_STOCKS_MDS.RAW.FINNHUB_STOCK_QUOTES_STAGE
→ FINNHUB_STOCKS_MDS.RAW.RAW_STOCK_QUOTES
```

---

## 4. Tools used in this phase

| Tool                          | Purpose                                     |
| ----------------------------- | ------------------------------------------- |
| Amazon S3                     | Stores raw JSONL stock quote files          |
| AWS IAM Policy                | Gives read-only S3 permissions              |
| AWS IAM Role                  | Allows Snowflake to access S3 securely      |
| Snowflake Storage Integration | Secure connection between Snowflake and S3  |
| Snowflake External Stage      | Points Snowflake to the S3 file location    |
| Snowflake Raw Table           | Stores raw JSON records loaded from S3      |
| SQL Worksheet                 | Used to create objects and validate loading |

---

## 5. S3 source location

The S3 bucket is:

```text
finnhub-stocks
```

The raw stock quote files are stored under:

```text
s3://finnhub-stocks/raw/stock_quotes/
```

Example loaded file:

```text
s3://finnhub-stocks/raw/stock_quotes/ingestion_date=2026-06-29/hour=07/batch_20260629T075131Z.jsonl
```

The file format is JSONL.

JSONL means:

```text
One JSON object per line
```

Example:

```json
{"symbol": "MSFT", "current_price": 372.97, "change": 20.14, "percent_change": 5.7081, "fetched_at": "2026-06-29T06:10:05.772942+00:00"}
{"symbol": "GOOGL", "current_price": 337.39, "change": -6.32, "percent_change": -1.8388, "fetched_at": "2026-06-29T06:10:06.707984+00:00"}
```

---

## 6. Snowflake database structure

The Snowflake database created for this project is:

```text
FINNHUB_STOCKS_MDS
```

Schemas:

```text
FINNHUB_STOCKS_MDS
├── RAW
├── STAGING
├── INTERMEDIATE
└── MARTS
```

Purpose of each schema:

| Schema       | Purpose                                                |
| ------------ | ------------------------------------------------------ |
| RAW          | Stores data loaded from S3 with minimal transformation |
| STAGING      | Future dbt staging models, such as `stg_stock_quotes`  |
| INTERMEDIATE | Future dbt models for business logic                   |
| MARTS        | Future analytics-ready fact and dimension tables       |

---

## 7. Snowflake setup script

File suggestion:

```text
sql/01_setup_snowflake_objects.sql
```

SQL:

```sql
-- 01_setup_snowflake_objects.sql
-- Purpose:
-- Create the Snowflake database, schemas, and cost-controlled warehouse.

CREATE DATABASE IF NOT EXISTS finnhub_stocks_mds;


CREATE WAREHOUSE IF NOT EXISTS finnhub_stocks_mds_wh
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;


USE WAREHOUSE finnhub_stocks_mds_wh;
USE DATABASE finnhub_stocks_mds;


-- Store JSONL records loaded from S3 with minimal/no transformation
CREATE SCHEMA IF NOT EXISTS finnhub_stocks_mds.raw;

-- Future dbt layer: flatten JSON fields, rename columns, cast data types
CREATE SCHEMA IF NOT EXISTS finnhub_stocks_mds.staging;

-- Future dbt layer: add business logic
CREATE SCHEMA IF NOT EXISTS finnhub_stocks_mds.intermediate;

-- Future dbt layer: create analytics-friendly dashboard tables
CREATE SCHEMA IF NOT EXISTS finnhub_stocks_mds.marts;


USE SCHEMA finnhub_stocks_mds.raw;
```

Validation query:

```sql
SELECT
    CURRENT_ROLE() AS current_role,
    CURRENT_WAREHOUSE() AS current_warehouse,
    CURRENT_DATABASE() AS current_database,
    CURRENT_SCHEMA() AS current_schema;
```

Expected result:

```text
CURRENT_WAREHOUSE  = FINNHUB_STOCKS_MDS_WH
CURRENT_DATABASE   = FINNHUB_STOCKS_MDS
CURRENT_SCHEMA     = RAW
```

---

## 8. Raw table and JSONL file format

File suggestion:

```text
sql/02_create_raw_stock_quotes_table.sql
```

SQL:

```sql
-- 02_create_raw_stock_quotes_table.sql
-- Purpose:
-- Create the raw Snowflake table and JSONL file format
-- for stock quote records loaded from Amazon S3.

USE WAREHOUSE finnhub_stocks_mds_wh;
USE DATABASE finnhub_stocks_mds;
USE SCHEMA raw;


CREATE TABLE IF NOT EXISTS raw_stock_quotes (
    raw_record VARIANT,
    source_file_name STRING,
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


CREATE FILE FORMAT IF NOT EXISTS jsonl_file_format
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE;


SHOW TABLES IN SCHEMA finnhub_stocks_mds.raw;

SHOW FILE FORMATS IN SCHEMA finnhub_stocks_mds.raw;
```

Created objects:

```text
FINNHUB_STOCKS_MDS.RAW.RAW_STOCK_QUOTES
FINNHUB_STOCKS_MDS.RAW.JSONL_FILE_FORMAT
```

The raw table has three columns:

| Column           | Type          | Purpose                                          |
| ---------------- | ------------- | ------------------------------------------------ |
| raw_record       | VARIANT       | Stores the full JSON object from each JSONL line |
| source_file_name | STRING        | Stores the S3 file path                          |
| loaded_at        | TIMESTAMP_NTZ | Stores when the row was loaded into Snowflake    |

---

## 9. AWS IAM policy

A custom AWS IAM policy was created to give Snowflake read-only access to the S3 raw stock quote folder.

Policy name:

```text
snowflake_finnhub_stocks_s3_read_policy
```

Policy JSON:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowBucketLocation",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::finnhub-stocks"
    },
    {
      "Sid": "AllowListStockQuotePrefix",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::finnhub-stocks",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "raw/stock_quotes/",
            "raw/stock_quotes/*"
          ]
        }
      }
    },
    {
      "Sid": "AllowReadStockQuoteFiles",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::finnhub-stocks/raw/stock_quotes/*"
    }
  ]
}
```

This policy only allows Snowflake to read files from:

```text
s3://finnhub-stocks/raw/stock_quotes/
```

It does not give full access to the whole AWS account.

---

## 10. AWS IAM role

An IAM role was created for Snowflake.

Role name:

```text
snowflake_finnhub_stocks_s3_read_role
```

This role has the custom S3 read policy attached:

```text
snowflake_finnhub_stocks_s3_read_policy
```

The role ARN follows this format:

```text
arn:aws:iam::<aws-account-id>:role/snowflake_finnhub_stocks_s3_read_role
```

This ARN was later used in Snowflake when creating the storage integration.

Do not hardcode real AWS credentials into GitHub.

---

## 11. Snowflake storage integration

File suggestion:

```text
sql/03_create_s3_storage_integration.sql
```

SQL:

```sql
-- 03_create_s3_storage_integration.sql
-- Purpose:
-- Create a secure Snowflake storage integration for reading
-- raw Finnhub stock quote JSONL files from Amazon S3.

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS finnhub_stocks_s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<aws-account-id>:role/snowflake_finnhub_stocks_s3_read_role'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('s3://finnhub-stocks/raw/stock_quotes/');
```

After creating the storage integration, this command was used:

```sql
DESC INTEGRATION finnhub_stocks_s3_integration;
```

This returned two important generated values from Snowflake:

```text
STORAGE_AWS_IAM_USER_ARN
STORAGE_AWS_EXTERNAL_ID
```

Those values were used to update the AWS IAM role trust relationship.

---

## 12. AWS trust relationship update

The IAM role trust relationship was updated so Snowflake can assume the role.

The trust policy follows this structure:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<STORAGE_AWS_IAM_USER_ARN_FROM_SNOWFLAKE>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID_FROM_SNOWFLAKE>"
        }
      }
    }
  ]
}
```

Important:

```text
Principal AWS = STORAGE_AWS_IAM_USER_ARN from Snowflake
ExternalId    = STORAGE_AWS_EXTERNAL_ID from Snowflake
```

This completed the secure connection between Snowflake and AWS S3.

---

## 13. Snowflake external stage

File suggestion:

```text
sql/04_create_s3_external_stage.sql
```

SQL:

```sql
-- 04_create_s3_external_stage.sql
-- Purpose:
-- Create an external stage that points Snowflake to the raw JSONL files in Amazon S3.

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE finnhub_stocks_mds_wh;
USE DATABASE finnhub_stocks_mds;
USE SCHEMA raw;


CREATE STAGE IF NOT EXISTS finnhub_stock_quotes_stage
    URL = 's3://finnhub-stocks/raw/stock_quotes/'
    STORAGE_INTEGRATION = finnhub_stocks_s3_integration
    FILE_FORMAT = jsonl_file_format;
```

Created stage:

```text
FINNHUB_STOCKS_MDS.RAW.FINNHUB_STOCK_QUOTES_STAGE
```

Validation query:

```sql
LIST @finnhub_stock_quotes_stage;
```

Result:

```text
s3://finnhub-stocks/raw/stock_quotes/ingestion_date=2026-06-29/hour=07/batch_20260629T075131Z.jsonl
```

This confirmed that Snowflake could see the S3 JSONL file.

---

## 14. Loading S3 data into the raw table

File suggestion:

```text
sql/05_copy_s3_to_raw_stock_quotes.sql
```

SQL:

```sql
-- 05_copy_s3_to_raw_stock_quotes.sql
-- Purpose:
-- Load JSONL records from the S3 external stage into the Snowflake raw table.

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE finnhub_stocks_mds_wh;
USE DATABASE finnhub_stocks_mds;
USE SCHEMA raw;


COPY INTO raw_stock_quotes (raw_record, source_file_name)
FROM (
    SELECT
        $1 AS raw_record,
        METADATA$FILENAME AS source_file_name
    FROM @finnhub_stock_quotes_stage
)
FILE_FORMAT = (FORMAT_NAME = jsonl_file_format)
ON_ERROR = 'CONTINUE';
```

COPY result:

```text
STATUS       = LOADED
ROWS_PARSED  = 25
ROWS_LOADED  = 25
ERRORS_SEEN  = 0
```

This confirmed that the S3 JSONL file was loaded successfully into Snowflake.

---

## 15. Raw table validation

Row count validation:

```sql
SELECT COUNT(*) AS total_rows
FROM raw_stock_quotes;
```

Result:

```text
25
```

Sample row validation:

```sql
SELECT *
FROM raw_stock_quotes
LIMIT 5;
```

Result columns:

```text
RAW_RECORD
SOURCE_FILE_NAME
LOADED_AT
```

This confirmed that the raw JSON objects were stored in Snowflake.

---

## 16. JSON field extraction validation

The raw table stores JSON in the `RAW_RECORD` column.

To check that Snowflake can read the JSON fields correctly, this validation query was used:

```sql
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
```

Example output:

```text
SYMBOL | CURRENT_PRICE | PRICE_CHANGE | PERCENT_CHANGE | FETCHED_AT
MSFT   | 372.97        | 20.14        | 5.7081         | 2026-06-29 06:10:05
GOOGL  | 337.39        | -6.32        | -1.8388        | 2026-06-29 06:10:06
AMZN   | 232.69        | 5.68         | 2.5021         | 2026-06-29 06:10:07
AAPL   | 283.78        | 8.63         | 3.1365         | 2026-06-29 06:11:39
```

This confirmed that the raw JSON can be flattened into normal analytical columns.

This query is not the final dbt staging model yet, but it proves that the future staging model can be built from `RAW_STOCK_QUOTES`.

---

## 17. Current completed status

Completed in this phase:

```text
✅ Snowflake database created
✅ Snowflake warehouse created
✅ RAW, STAGING, INTERMEDIATE, and MARTS schemas created
✅ Raw stock quotes table created
✅ JSONL file format created
✅ AWS IAM read policy created
✅ AWS IAM role created
✅ Snowflake storage integration created
✅ AWS trust relationship updated
✅ Snowflake external stage created
✅ Snowflake can list S3 files
✅ S3 JSONL file loaded into RAW_STOCK_QUOTES
✅ 25 rows loaded successfully
✅ JSON fields validated successfully
```

Current completed pipeline:

```text
Finnhub API
→ Kafka
→ Amazon S3
→ Snowflake raw table
```

---

## 18. Current Snowflake objects

Database:

```text
FINNHUB_STOCKS_MDS
```

Warehouse:

```text
FINNHUB_STOCKS_MDS_WH
```

Schemas:

```text
RAW
STAGING
INTERMEDIATE
MARTS
```

Raw table:

```text
FINNHUB_STOCKS_MDS.RAW.RAW_STOCK_QUOTES
```

File format:

```text
FINNHUB_STOCKS_MDS.RAW.JSONL_FILE_FORMAT
```

Storage integration:

```text
FINNHUB_STOCKS_S3_INTEGRATION
```

External stage:

```text
FINNHUB_STOCKS_MDS.RAW.FINNHUB_STOCK_QUOTES_STAGE
```

---

## 19. Important design decision

The tutorial used a MinIO-to-Snowflake flow.

This project uses real Amazon S3 instead.

The tutorial approach was closer to:

```text
MinIO
→ Airflow downloads files locally
→ Snowflake internal stage
→ Snowflake table
```

This project uses:

```text
Amazon S3
→ Snowflake storage integration
→ Snowflake external stage
→ Snowflake raw table
```

This is more suitable for a cloud-based modern data stack project because Snowflake can read directly from S3.

---

## 20. Relationship to dbt layers

This phase created the raw landing layer in Snowflake.

The project requirement uses these dbt layers:

```text
stg_*  = staging layer
int_*  = intermediate layer
fct_* / dim_* = mart layer
```

The raw table is before dbt.

The next dbt flow will be:

```text
RAW.RAW_STOCK_QUOTES
→ STAGING.STG_STOCK_QUOTES
→ INTERMEDIATE.INT_STOCK_PRICE_METRICS
→ MARTS.FCT_STOCK_QUOTES
→ MARTS.DIM_STOCK_SYMBOL
```

The staging model will use the same extraction logic that was validated manually:

```sql
raw_record:symbol::STRING AS symbol,
raw_record:current_price::FLOAT AS current_price,
raw_record:fetched_at::TIMESTAMP_NTZ AS fetched_at
```

---

## 21. Next phase

The next phase is to create the dbt project and build the first staging model.

Next target:

```text
RAW.RAW_STOCK_QUOTES
→ dbt model: stg_stock_quotes
```

The staging model should:

```text
- Flatten JSON fields
- Rename columns clearly
- Cast data types
- Keep transformations minimal
- Add basic data quality tests later
```

