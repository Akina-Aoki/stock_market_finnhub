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


> start the local environment with Docker Compose. Kafka and Zookeeper are started first because Kafka depends on Zookeeper. Then I start the remaining services, including Airflow webserver, Airflow scheduler, Postgres, and Kafdrop.

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


> check that Airflow can detect my DAG and that there are no import errors. This confirms that the DAG file is valid and ready to run.

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

> Airflow orchestrates the full workflow. It creates a run ID, runs the producer and consumer, loads data from S3 into Snowflake, checks raw data quality, runs dbt transformations, runs dbt tests, and validates that the marts layer has data.

### 5.1 Terminal proof of DAG run

```bash
docker exec -it airflow-scheduler airflow dags list-runs -d finnhub_daily_stock_pipeline
```

Expected latest run state:

```text
success
```


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

Check

1. Project models
2. Lineage graph
3. `dim_date`
4. `dim_stock_symbol`
5. `fct_stock_quotes_daily`
6. Model descriptions and tests


> dbt docs documents the transformation layer. The lineage graph shows how the data moves from the raw source into staging, then intermediate, and finally into the marts layer. This helps explain dependencies and makes the project easier to understand and maintain.

To stop the docs server:

```text
Ctrl + C
```
