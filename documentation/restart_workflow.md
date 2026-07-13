# Finnhub Modern Data Stack Demo Guide

### 1. Prerequisites

Make sure you have these installed and available from your terminal:

- Docker Desktop or Docker Engine with Docker Compose
- Python virtual environment for this project
- Terraform CLI
- dbt dependencies from `requirements.txt`
- A local `.env` file in the project root

The `.env` file is required because Docker Compose, Airflow, dbt, and the Python scripts read local credentials and connection settings from it. Keep it on your machine only. Do not commit `.env` to Git.

---

This project is a small-scale modern data stack pipeline for Finnhub stock quote data.

The pipeline ingests stock quote data, stores raw files in Amazon S3, loads the data into Snowflake, transforms it through dbt layers, runs data quality tests, and orchestrates the workflow with Airflow.

The main tools are:

| Tool | Role in the project |
|---|---|
| Docker | Runs the local services such as Airflow, Kafka, Zookeeper, Postgres, and Kafdrop |
| Airflow | Orchestrates the end-to-end pipeline |
| Amazon S3 | Stores raw Finnhub JSON data |
| Snowflake | Stores the data warehouse layers |
| dbt | Builds staging, intermediate, and marts models, runs tests, and generates documentation |
| Terraform | Defines and validates the cloud infrastructure setup for S3 and Snowflake |


> Terraform prepares the cloud infrastructure, Docker runs the local services, Airflow controls the workflow, and dbt transforms and tests the data in Snowflake.

---

## 2. Restart workflow commands

Run these commands when starting the project from a stopped Docker state.

### 2.1 Move to the project root

```bash
cd ~/de25/stock_market_finnhub
```

### 2.2 Activate the virtual environment

```bash
source .venv_stock/Scripts/activate
```

### 2.3 Stop existing containers if needed

```bash
docker compose stop
```

Check that no containers are running:

```bash
docker ps
```

Expected result:

```text
CONTAINER ID   IMAGE   COMMAND   CREATED   STATUS   PORTS   NAMES
```

This means no containers are currently running.

---

## 3. Start Docker services

### 3.1 Start Zookeeper and Kafka first

```bash
docker compose up -d zookeeper kafka
```

Check container status:

```bash
docker ps
```

At first, Kafka may show:

```text
health: starting
```

Wait until it changes to:

```text
healthy
```

### 3.2 Start the remaining services

```bash
docker compose up -d
```

Check all running containers:

```bash
docker ps
```

Expected services:

| Service | Purpose | Local URL / Port |
|---|---|---|
| zookeeper | Kafka dependency | 2181 |
| kafka | Message broker | 9092 / 29092 |
| kafdrop | Kafka UI | http://localhost:9000 |
| airflow-postgres | Airflow metadata database | 5432 |
| airflow-scheduler | Runs scheduled Airflow tasks | internal |
| airflow-webserver | Airflow UI | http://localhost:8080 |

What to say:

> I start the local environment with Docker Compose. Kafka and Zookeeper are started first because Kafka depends on Zookeeper. Then I start the remaining services, including Airflow webserver, Airflow scheduler, Postgres, and Kafdrop.

---

## 4. Validate Airflow

### 4.1 Check that Airflow can see the DAG

```bash
docker exec -it airflow-scheduler airflow dags list | grep finnhub
```

Expected DAG:

```text
finnhub_daily_stock_pipeline
```

If the paused column shows:

```text
False
```

that is good. It means the DAG is not paused.

### 4.2 Check DAG import errors

```bash
docker exec -it airflow-scheduler airflow dags list-import-errors
```

Expected result:

```text
No data found
```


> I check that Airflow can detect my DAG and that there are no import errors. This confirms that the DAG file is valid and ready to run.

---

## 5. Trigger the DAG from the Airflow UI

Open:

```text
http://localhost:8080
```

Steps:

1. Find the DAG: `finnhub_daily_stock_pipeline`
2. Make sure it is unpaused
3. Click the trigger/play button
4. Open the DAG run
5. Watch the task flow until all tasks are green

Main DAG tasks to show:

```text
create_run_id
run_producer_once
run_consumer_once
copy_s3_to_snowflake
check_raw_batch_quality
dbt_run
dbt_test
check_mart_row_count
end
```

What to say:

> Airflow orchestrates the full workflow. It creates a run ID, runs the producer and consumer, loads data from S3 into Snowflake, checks raw data quality, runs dbt transformations, runs dbt tests, and validates that the marts layer has data.

### 5.1 Terminal proof of DAG run

```bash
docker exec -it airflow-scheduler airflow dags list-runs -d finnhub_daily_stock_pipeline
```

Expected latest run state:

```text
success
```

What to say:

> The latest manual DAG run completed successfully, which proves that the restarted workflow works end-to-end.

---

## 6. Validate dbt locally

Before running dbt locally, load the environment variables from `.env`.

Do not print or paste the contents of `.env`.

### 6.1 Load environment variables

From the project root:

```bash
cd ~/de25/stock_market_finnhub

set -a
source .env
set +a
```

Optional safe check:

```bash
echo ${SNOWFLAKE_ACCOUNT:+SNOWFLAKE_ACCOUNT_loaded}
```

Expected result:

```text
SNOWFLAKE_ACCOUNT_loaded
```

### 6.2 Move to dbt project

```bash
cd ~/de25/stock_market_finnhub/finnhub_stocks
```

### 6.3 Check dbt connection

```bash
dbt debug
```

Expected result:

```text
Connection test: [OK connection ok]
All checks passed!
```

What to say:

> dbt debug confirms that my local dbt project can connect to Snowflake using the configured profile and environment variables.

---

## 7. Run dbt models and tests

### 7.1 Run dbt models

```bash
dbt run
```

Expected successful result:

```text
Completed successfully
PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5
```

Models built:

```text
STAGING.stg_stock_quotes
INTERMEDIATE.int_stock_quote_metrics
MARTS.dim_date
MARTS.dim_stock_symbol
MARTS.fct_stock_quotes_daily
```

What to say:

> dbt run builds the transformation models across staging, intermediate, and marts. The staging and intermediate models are views, while the marts models are materialized as analytics-ready tables.

### 7.2 Run dbt tests

```bash
dbt test
```

Expected successful result:

```text
Finished running 68 data tests
Completed successfully
```

What to say:

> dbt test validates data quality. The project includes tests such as not_null, unique, and relationship tests where applicable.

---

## 8. Generate and serve dbt docs

### 8.1 Generate docs

```bash
dbt docs generate
```

Expected result:

```text
Catalog written to .../target/catalog.json
```

### 8.2 Serve docs locally

```bash
dbt docs serve --port 8081
```

Open:

```text
http://localhost:8081
```

What to show in dbt docs:

1. Project models
2. Lineage graph
3. `dim_date`
4. `dim_stock_symbol`
5. `fct_stock_quotes_daily`
6. Model descriptions and tests

What to say:

> dbt docs documents the transformation layer. The lineage graph shows how the data moves from the raw source into staging, then intermediate, and finally into the marts layer. This helps explain dependencies and makes the project easier to understand and maintain.

To stop the docs server:

```text
Ctrl + C
```

---

# Demo Order

Use this order during the actual presentation.

---

## 1. GitHub repository

Show:

- Project folder structure
- Airflow DAG file
- dbt project folder
- Terraform folder
- README or review markdown

What to say:

> This repository contains the complete project setup: ingestion scripts, Airflow DAG, dbt models, Terraform infrastructure code, and documentation.

---

## 2. Amazon S3 raw storage

Open the S3 bucket:

```text
finnhub-stocks
```

Show folders like:

```text
raw/stock_quotes/ingestion_date=...
```

What to say:

> S3 is used as the raw storage layer. Each pipeline run saves raw Finnhub stock quote data as JSON files, organized by ingestion date.

---

## 3. Snowflake RAW layer

Use database:

```sql
USE DATABASE FINNHUB_STOCKS_MDS;
USE SCHEMA RAW;
```

Show the RAW table:

```sql
SHOW TABLES IN SCHEMA FINNHUB_STOCKS_MDS.RAW;
```

Main RAW table:

```text
RAW_STOCK_QUOTES
```

Preview loaded source files:

```sql
SELECT
    SOURCE_FILE_NAME,
    COUNT(*) AS row_count,
    MIN(LOADED_AT) AS first_loaded_at,
    MAX(LOADED_AT) AS last_loaded_at
FROM FINNHUB_STOCKS_MDS.RAW.RAW_STOCK_QUOTES
GROUP BY SOURCE_FILE_NAME
ORDER BY SOURCE_FILE_NAME;
```

What to say:

> The RAW layer stores the original semi-structured records loaded from S3. I also keep metadata such as the source file name and loaded timestamp, so I can trace which S3 files were loaded into Snowflake.

---

## 4. Snowflake STAGING layer

```sql
USE DATABASE FINNHUB_STOCKS_MDS;
USE SCHEMA STAGING;

SHOW VIEWS IN SCHEMA FINNHUB_STOCKS_MDS.STAGING;

SELECT *
FROM FINNHUB_STOCKS_MDS.STAGING.STG_STOCK_QUOTES
LIMIT 20;
```

What to say:

> The staging layer reads from the raw table and converts the raw JSON records into structured columns such as symbol, current price, high price, low price, open price, previous close, fetch timestamp, source file name, and loaded timestamp.

Columns to point out:

```text
SYMBOL
CURRENT_PRICE
PRICE_CHANGE
PRICE_CHANGE_PERCENT
HIGH_PRICE
LOW_PRICE
OPEN_PRICE
PREVIOUS_CLOSE_PRICE
FINNHUB_TIMESTAMP
FETCHED_AT
SOURCE_FILE_NAME
LOADED_AT
```

---

## 5. Snowflake INTERMEDIATE layer

```sql
USE DATABASE FINNHUB_STOCKS_MDS;
USE SCHEMA INTERMEDIATE;

SHOW VIEWS IN SCHEMA FINNHUB_STOCKS_MDS.INTERMEDIATE;

SELECT *
FROM FINNHUB_STOCKS_MDS.INTERMEDIATE.INT_STOCK_QUOTE_METRICS
LIMIT 20;
```

What to say:

> The intermediate layer builds on staging and adds business-friendly metrics. This includes fetched date, fetched hour, daily price range, daily price range percentage, price movement direction, and price position inside the daily range.

Columns to point out:

```text
FETCHED_DATE
FETCHED_HOUR
DAILY_PRICE_RANGE
DAILY_PRICE_RANGE_PERCENT
PRICE_MOVEMENT_DIRECTION
PRICE_POSITION_IN_DAILY_RANGE
```

---

## 6. Snowflake MARTS layer

```sql
USE DATABASE FINNHUB_STOCKS_MDS;
USE SCHEMA MARTS;

SHOW TABLES IN SCHEMA FINNHUB_STOCKS_MDS.MARTS;
```

Expected mart tables:

```text
DIM_DATE
DIM_STOCK_SYMBOL
FCT_STOCK_QUOTES_DAILY
```

### 6.1 Dimension table: stock symbol

```sql
SELECT *
FROM FINNHUB_STOCKS_MDS.MARTS.DIM_STOCK_SYMBOL
LIMIT 20;
```

What to say:

> The stock symbol dimension stores descriptive information about each stock symbol. It connects to the fact table through stock_symbol_id.

### 6.2 Dimension table: date

```sql
SELECT *
FROM FINNHUB_STOCKS_MDS.MARTS.DIM_DATE
ORDER BY DATE_DAY
LIMIT 20;
```

What to say:

> The date dimension stores date-related attributes. This makes the fact table easier to analyze by day, month, year, or weekday.

### 6.3 Fact table: daily stock quotes

```sql
SELECT *
FROM FINNHUB_STOCKS_MDS.MARTS.FCT_STOCK_QUOTES_DAILY
ORDER BY date_id, stock_symbol_id;
```

What to say:

> This is the final fact table in the marts layer. It stores daily stock quote metrics at the grain of one stock symbol per fetched date. The fact table connects to DIM_DATE through date_id and to DIM_STOCK_SYMBOL through stock_symbol_id.

Star schema explanation:

> The marts layer is the final analytics-ready layer. I created a simple star schema with dimension tables for dates and stock symbols, and a fact table for daily stock quote metrics. This structure is easier to query for reporting, dashboards, and analysis.

---

## 7. dbt docs

Open:

```text
http://localhost:8081
```

Show:

- Lineage graph
- Staging model
- Intermediate model
- Mart models
- Tests

What to say:

> After showing the actual tables in Snowflake, I use dbt docs to show how the transformation layer is documented. The docs make it easier to understand model dependencies, column definitions, and tests.

---

## 8. Airflow DAG

Open:

```text
http://localhost:8080
```

Show:

- DAG grid or graph view
- Latest successful manual run
- Green task flow

What to say:

> Airflow is responsible for orchestration. It runs the ingestion tasks, loads the data to Snowflake, runs dbt transformations, runs dbt tests, and performs a final mart row count check.

---

## 9. Terraform validation

From the Terraform folder:

```bash
cd ~/de25/stock_market_finnhub/terraform
terraform plan
```

Expected result:

```text
No changes. Your infrastructure matches the configuration.
```

What to say:

> Terraform is used as the optional Infrastructure as Code layer. It defines the cloud infrastructure, including the S3 bucket, Snowflake database, warehouse, and schemas. The final Terraform plan shows no changes, which confirms that the AWS and Snowflake infrastructure matches the Terraform configuration.

Terraform-managed resources:

```text
S3 bucket:
finnhub-stocks

Snowflake database:
FINNHUB_STOCKS_MDS

Snowflake warehouse:
FINNHUB_STOCKS_MDS_WH

Snowflake schemas:
RAW
STAGING
INTERMEDIATE
MARTS
```

---

# Final closing summary

Use this at the end of the demo:

> This project demonstrates a small end-to-end modern data stack. Raw data is ingested from the Finnhub API and stored in S3. Airflow orchestrates the workflow, Snowflake stores the data warehouse layers, and dbt transforms the data from raw into staging, intermediate, and marts. The marts layer uses a simple star schema with dimensions and a fact table. Data quality is validated with dbt tests, documentation is generated with dbt docs, and Terraform defines the cloud infrastructure as code.

---

## Quick checklist before demo

- [ ] Docker containers are running
- [ ] Kafka is healthy
- [ ] Airflow UI opens at `http://localhost:8080`
- [ ] DAG is visible and unpaused
- [ ] Latest DAG run is successful
- [ ] dbt debug passes
- [ ] dbt run passes
- [ ] dbt test passes
- [ ] dbt docs are served on `http://localhost:8081`
- [ ] S3 bucket shows raw files
- [ ] Snowflake shows RAW, STAGING, INTERMEDIATE, and MARTS layers
- [ ] Terraform plan shows `No changes`

---

## Notes to remember

- `created_on` in Snowflake `SHOW TABLES` means when the table was created, not the dates inside the data.
- S3 and Snowflake are separate. S3 shows raw files; Snowflake only shows data after the COPY/load task has loaded those files.
- `False` in the Airflow paused column means the DAG is not paused.
- `No data found` in Airflow import errors means there are no DAG import errors.
- The MARTS layer is tables, not views, because it is the final analytics-ready layer.
- Terraform does not run Docker, Airflow, or dbt. Terraform manages cloud infrastructure only.
