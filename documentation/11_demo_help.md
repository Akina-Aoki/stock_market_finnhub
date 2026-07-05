# Finnhub Modern Data Stack Demo Guide

## 1. Demo goal

Show that this project is a small end-to-end modern data pipeline:

```text
Finnhub API → Kafka → Consumer → S3 → Snowflake RAW → dbt → Snowflake MARTS → Airflow orchestration
```

Key message:

> This project demonstrates ingestion, orchestration, transformation, testing, and analytics-ready modeling using a local modern data stack.

---

## 2. Files to show in VS Code

### `docker-compose.yml`

Explain:

- Defines the local infrastructure.
- Starts Kafka, Zookeeper, Kafdrop, Airflow webserver, Airflow scheduler, and Airflow Postgres.
- Since this is local, scheduled runs only happen while Docker and the computer are running.

Key point:

> Docker Compose gives me a reproducible local environment for the pipeline.

---

### Airflow DAG file

Example:

```text
dags/02_finnhub_daily_pipeline.py
```

Explain the task flow:

```text
start
  ↓
create_run_id
  ↓
run_producer_once
  ↓
run_consumer_once
  ↓
copy_s3_to_snowflake
  ↓
check_raw_batch_quality
  ↓
dbt_run
  ↓
dbt_test
  ↓
check_mart_row_count
  ↓
end
```

Key point:

> Airflow is the orchestrator. It controls the order of tasks and makes the pipeline re-runnable.

---

### Producer script

Example:

```text
scripts/producer_*.py
```

Explain:

- Calls the Finnhub API.
- Gets stock quote data.
- Sends the quote messages to Kafka.

Key point:

> The producer extracts data from the external API and publishes it into the streaming layer.

---

### Consumer script

Example:

```text
scripts/consumer_*.py
```

Explain:

- Reads messages from Kafka.
- Saves raw JSON data to S3.
- Keeps the data in raw form for traceability.

Key point:

> The consumer moves messages from Kafka into cloud storage as raw source data.

---

### Snowflake load script

Example:

```text
scripts/copy_s3_to_snowflake*.py
```

Explain:

- Copies raw files from S3 into Snowflake RAW tables.
- This creates the warehouse landing layer.

Key point:

> Snowflake stores the raw and transformed data used by dbt.

---

## 3. dbt files to show

### `dbt_project.yml`

Explain:

- Defines the dbt project.
- Organizes models into layers: staging, intermediate, and marts.

Key point:

> dbt handles transformations and model structure.

---

### `models/staging/stg_stock_quotes.sql`

Explain:

- Cleans the raw Snowflake data.
- Standardizes column names and data types.
- Keeps transformations minimal.

Key point:

> Staging prepares raw data into a clean and consistent format.

---

### `models/intermediate/int_stock_quote_metrics.sql`

Explain:

- Adds business logic and calculated metrics.
- Examples: daily price range, price movement direction, price position in range.

Key point:

> Intermediate models contain reusable business logic.

---

### `models/marts/dim_stock_symbol.sql`

Explain:

- Dimension table for stock symbols.
- One row per stock symbol.

Key point:

> This dimension answers “what stock is this?”

---

### `models/marts/dim_date.sql`

Explain:

- Dimension table for dates.
- Supports filtering by year, month, day, and weekday.

Key point:

> This dimension supports time-based analysis.

---

### `models/marts/fct_stock_quotes_daily.sql`

Explain:

- Final analytics-ready fact table.
- One row per stock symbol per fetched date.
- Uses `stock_symbol_id` and `date_id` to connect to dimensions.

Key point:

> This is the main table for dashboarding and analysis.

Star schema:

```text
DIM_STOCK_SYMBOL
        |
        | stock_symbol_id
        ↓
FCT_STOCK_QUOTES_DAILY
        ↑
        | date_id
DIM_DATE
```

---

### `models/marts/schema.yml`

Explain:

- Contains model and column documentation.
- Defines dbt tests.
- Tests include `not_null`, `unique`, `relationships`, `accepted_values`, and custom logic checks.

Key point:

> dbt tests prove that the final data is reliable.

---

## 4. Airflow UI demo

Open:

```text
http://localhost:8080
```

Show:

- DAG name: `finnhub_daily_stock_pipeline`
- DAG is unpaused.
- Latest DAG run is green/successful.
- Grid view shows each task is successful.
- Logs are available per task.

Explain:

> The DAG controls the full workflow from ingestion to dbt testing. Each green task proves that one part of the pipeline completed successfully.

---

## 5. Snowflake demo

Show schemas:

```sql
SHOW TABLES IN SCHEMA FINNHUB_STOCKS_MDS.RAW;
SHOW TABLES IN SCHEMA FINNHUB_STOCKS_MDS.STAGING;
SHOW TABLES IN SCHEMA FINNHUB_STOCKS_MDS.INTERMEDIATE;
SHOW TABLES IN SCHEMA FINNHUB_STOCKS_MDS.MARTS;
```

Show mart data:

```sql
SELECT *
FROM FINNHUB_STOCKS_MDS.MARTS.DIM_STOCK_SYMBOL
ORDER BY symbol
LIMIT 10;
```

```sql
SELECT *
FROM FINNHUB_STOCKS_MDS.MARTS.DIM_DATE
ORDER BY date_day DESC
LIMIT 10;
```

```sql
SELECT *
FROM FINNHUB_STOCKS_MDS.MARTS.FCT_STOCK_QUOTES_DAILY
ORDER BY fetched_date DESC, symbol
LIMIT 20;
```

Explain:

> Snowflake is the warehouse. The mart layer is the final analytics-ready output of the pipeline.

---

## 6. dbt terminal demo

From the dbt project folder:

```bash
dbt debug
```

Explain:

> Checks that local dbt can connect to Snowflake.

```bash
dbt run
```

Explain:

> Builds or refreshes the dbt models in Snowflake.

```bash
dbt test
```

Explain:

> Runs data quality checks against the models.

Optional mart-only check:

```bash
dbt run --select dim_stock_symbol dim_date fct_stock_quotes_daily
```

```bash
dbt test --select dim_stock_symbol dim_date fct_stock_quotes_daily
```

Explain:

> This proves that the final star schema can be rebuilt and tested independently.

---

## 7. dbt docs demo

Generate docs:

```bash
dbt docs generate
```

Serve docs:

```bash
dbt docs serve --port 8081
```

Open:

```text
http://localhost:8081
```

Show:

- Lineage graph.
- Staging model.
- Intermediate model.
- Mart models.
- Column descriptions.
- Tests.

Explain:

> dbt docs make the data model understandable and transparent for other users.

---

## 8. How to explain local scheduling

Use this wording:

> The DAG is scheduled hourly, but this is a local Docker-based project. That means Airflow only runs scheduled jobs while my computer and Docker containers are active. In a production setup, Airflow would run on cloud infrastructure. For this project, I use manual triggers during demos to prove that the pipeline is fully re-runnable.

---

## 9. Final closing explanation

Use this summary:

> I built a local modern data stack pipeline using Docker, Kafka, S3, Snowflake, dbt, and Airflow. The pipeline extracts stock quote data from Finnhub, moves it through Kafka and S3, loads it into Snowflake, transforms it with dbt, validates it with dbt tests, and produces a final mart layer using a simple star schema. Airflow orchestrates the full workflow so the pipeline can run on schedule or be triggered manually.
