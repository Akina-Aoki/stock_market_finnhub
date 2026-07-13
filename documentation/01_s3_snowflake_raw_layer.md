# S3 → Snowflake Raw Layer

## ✅ Overview (**Show S3 bucket: finnhub-stocks/raw/stock_quotes/**)

So, the raw stock quote files are already stored in Amazon S3.

The next phase is to load those raw files into Snowflake warehouse.


To let Snowflake read files from S3 securely, I created an AWS IAM role and a Snowflake storage integration. 

Then I created an external stage in Snowflake, which points to the S3 folder where the raw JSONL files are stored.


The COPY INTO command loads the JSONL files from the external stage into the Snowflake RAW table.

The data first lands in the RAW schema before dbt cleans and transforms it in the next layers.


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


## ✅ What Snowflake does in this step (2. Show Snowflake RAW table: 06_raw.sql)

Snowflake reads the files from S3 and loads them into a raw table.

The raw table is:

```text
FINNHUB_STOCKS_MDS.RAW.RAW_STOCK_QUOTES
```

This table stores:

| Column | Simple meaning |
|---|---|
| `RAW_RECORD` | The full original JSONL object |
| `SOURCE_FILE_NAME` | The S3 file path where the row came from |
| `LOADED_AT` | The time when Snowflake loaded the row. Can trace where each row came from. |



| Layer | Purpose |
|---|---|
| RAW | Stores original JSON records from S3 and  keeps the original data mostly unchanged |
| STAGING | Extracts and cleans JSON fields into normal columns such as symbol, current price, high price, low price, and fetched timestamp. |
| INTERMEDIATE | Adds calculated metrics and business logic |
| MARTS | Creates analytics-ready fact and dimension tables |



## ✅ How I validate that the load worked  (still in 06_raw.sql)

I validate the load in three ways:

1. Check that Snowflake can list the files in the S3 stage
2. Check that rows were loaded into the raw table
3. Check that JSON fields can be extracted from `RAW_RECORD`


## Then move to dbt staging:

```text
STAGING.STG_STOCK_QUOTES
```


Now that the raw JSON data is loaded into Snowflake, the next step is dbt staging, where the JSON fields are extracted and cleaned into normal columns.
