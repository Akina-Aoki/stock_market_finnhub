# S3 → Snowflake Raw Layer: Simple Demo Explanation 

## ✅ Overview (**Show S3 bucket: finnhub-stocks/raw/stock_quotes/**)

In this part of the project, the raw stock quote files are already stored in Amazon S3.

The goal is to load those raw files into Snowflake, so Snowflake can become the main data warehouse for the project.

Simple flow:

```text
Amazon S3
→ Snowflake external stage
→ Snowflake RAW table
→ dbt staging
→ dbt intermediate
→ dbt marts
```

 In this phase, I move the raw JSONL files from Amazon S3 into Snowflake. S3 is used as the raw file storage, and Snowflake is used as the data warehouse. The data first lands in the RAW schema before dbt cleans and transforms it in the next layers.


## ✅ Why I use S3 before Snowflake

I use S3 as the raw storage layer. This means I keep the original JSONL files before transforming them. If something goes wrong later, I can always go back to the original raw files.


## ✅ What Snowflake does in this step (**2. Show Snowflake RAW table: 06_raw.sql**)

Snowflake reads the files from S3 and loads them into a raw table.

The raw table is:

```text
FINNHUB_STOCKS_MDS.RAW.RAW_STOCK_QUOTES
```

This table stores:

| Column | Simple meaning |
|---|---|
| `RAW_RECORD` | The full original JSON object |
| `SOURCE_FILE_NAME` | The S3 file path where the row came from |
| `LOADED_AT` | The time when Snowflake loaded the row |


> The RAW table keeps the original JSON object in one column called RAW_RECORD. I also keep the source file name and loaded timestamp so I can trace where each row came from.

---

## ✅ Important: RAW table is not the same as STAGING

The RAW table keeps the original data mostly unchanged.

The STAGING layer is where dbt later extracts the JSON fields into clean columns.

Simple difference:

| Layer | Purpose |
|---|---|
| RAW | Stores original JSON records from S3 |
| STAGING | Extracts and cleans JSON fields into normal columns |
| INTERMEDIATE | Adds calculated metrics and business logic |
| MARTS | Creates analytics-ready fact and dimension tables |


> The RAW layer is before dbt transformation. It stores the data mostly as it arrived. The STAGING layer comes after RAW and turns the JSON fields into clean columns such as symbol, current price, high price, low price, and fetched timestamp.

---

## ✅ Snowflake database structure

The Snowflake database is:

```text
FINNHUB_STOCKS_MDS
```

The project uses these schemas:

```text
RAW
STAGING
INTERMEDIATE
MARTS
```


> I separated the Snowflake database into layers. RAW is for loaded source data, STAGING is for cleaned data, INTERMEDIATE is for business logic, and MARTS is for final analytics-ready tables.

---

## ✅ How Snowflake connects to S3

Snowflake does not use my personal AWS secret key directly.

Instead, the connection is done securely using:

```text
AWS IAM Role
+ Snowflake Storage Integration
+ Snowflake External Stage
```


| Object | Easy explanation |
|---|---|
| AWS IAM Role | Gives Snowflake permission to read the S3 folder |
| Storage Integration | Secure connection setup between Snowflake and S3 |
| External Stage | Snowflake object that points to the S3 file location |

What to say:

> To let Snowflake read files from S3 securely, I created an AWS IAM role and a Snowflake storage integration. Then I created an external stage in Snowflake, which points to the S3 folder where the raw JSONL files are stored.

---

## ✅ S3 location

The S3 bucket is:

```text
finnhub-stocks
```

The raw files are stored here:

```text
s3://finnhub-stocks/raw/stock_quotes/
```

Example structure:

```text
raw/stock_quotes/
└── ingestion_date=2026-07-05/
    └── hour=14/
        └── batch_20260705T144653Z.jsonl
```

What to say:

> The files in S3 are organized by ingestion date and hour. This makes it easier to track when the data was collected.

---

## ✅ What JSONL means

The files are saved as JSONL.

JSONL means:

```text
One JSON object per line
```

Example:

```json
{"symbol": "AAPL", "current_price": 283.78, "fetched_at": "2026-07-05T14:46:53+00:00"}
{"symbol": "MSFT", "current_price": 372.97, "fetched_at": "2026-07-05T14:46:54+00:00"}
```

What to say:

> The consumer saves the data as JSONL. This is useful for data pipelines because each line is one separate record, so Snowflake can load the file line by line.

---

## ✅ Loading S3 data into Snowflake

The Snowflake loading step uses `COPY INTO`.

Simple flow:

```text
S3 JSONL files
→ Snowflake external stage
→ COPY INTO command
→ RAW_STOCK_QUOTES table
```

What to say:

> The COPY INTO command loads the JSONL files from the external stage into the Snowflake RAW table. This is the step that moves the data from S3 into the warehouse.

---

## ✅ How I validate that the load worked

I validate the load in three ways:

1. Check that Snowflake can list the files in the S3 stage
2. Check that rows were loaded into the raw table
3. Check that JSON fields can be extracted from `RAW_RECORD`

Useful demo queries:

```sql
USE DATABASE FINNHUB_STOCKS_MDS;
USE SCHEMA RAW;

SHOW TABLES;
```

```sql
SELECT COUNT(*) AS total_rows
FROM RAW_STOCK_QUOTES;
```

```sql
SELECT *
FROM RAW_STOCK_QUOTES
LIMIT 5;
```

```sql
SELECT
    RAW_RECORD:symbol::STRING AS symbol,
    RAW_RECORD:current_price::FLOAT AS current_price,
    RAW_RECORD:fetched_at::TIMESTAMP_NTZ AS fetched_at,
    SOURCE_FILE_NAME,
    LOADED_AT
FROM RAW_STOCK_QUOTES
LIMIT 10;
```

What to say:

> I validate the raw load by checking the row count, previewing the raw table, and extracting fields from the JSON. This proves that Snowflake can read the JSON data correctly and that the staging model can be built from this raw table.

---

## ✅ Why this design is useful

This design is useful because:

```text
1. S3 keeps the original raw files
2. Snowflake stores the raw data in the warehouse
3. dbt can transform the data after it lands in Snowflake
4. The pipeline is easier to debug because each layer has a clear purpose
```


This design keeps the pipeline organized. S3 stores the raw files, Snowflake loads them into the RAW layer, and dbt handles the transformations afterward. This makes the pipeline easier to debug, explain, and maintain.




In this step, the consumer has already saved raw Finnhub stock quote data into Amazon S3 as JSONL files. Snowflake connects to that S3 folder using a secure storage integration and an external stage. Then the COPY INTO command loads the JSONL records into the RAW_STOCK_QUOTES table. The RAW table keeps the original JSON record, the source file name, and the loaded timestamp. After this, dbt uses the raw table to build the staging, intermediate, and marts layers.



## Then move to dbt staging:

```text
STAGING.STG_STOCK_QUOTES
```


Now that the raw JSON data is loaded into Snowflake, the next step is dbt staging, where the JSON fields are extracted and cleaned into normal columns.
