# Checklist

| Requirement              |        Status | Notes                                                                                                           |
| ------------------------ | ------------: | --------------------------------------------------------------------------------------------------------------- |
| External data ingestion  | ✅ Mostly done | Finnhub API ingestion and raw data loaded into Snowflake. Need Airflow to run it automatically.                |
| Raw / landing layer      |        ✅ Done | `RAW.RAW_STOCK_QUOTES` exists in Snowflake. Dbt catalog shows the raw source with 25 rows.                 |
| Staging layer            |        ✅ Done | `stg_stock_quotes` exists as a Snowflake view in the `STAGING` schema.                                          |
| Intermediate layer       |        ✅ Done | `int_stock_quote_metrics` exists as a Snowflake view in the `INTERMEDIATE` schema.                              |
| Mart layer               |        ✅ Done | Built `dim_stock_symbol`, `dim_date`, and `fct_stock_quotes_daily`.                                         |
| Star schema              |        ✅ Done | Mart layer uses two dimensions and one fact table: `dim_stock_symbol → fct_stock_quotes_daily ← dim_date`.  |
| dbt tests                |        ✅ Done | Latest `dbt build --select marts` all passed and `ERROR=0`.                                        |
| dbt docs                 |        ✅ Done | Generated and served dbt docs on port `8081`.                                                               |
| Airflow orchestration    |       🔜 Next | Airflow exists, but still need the real project DAG.                                                         |
| README final explanation |      🔜 Later | Update README after Airflow is working.                                                               |
| Dashboard / outputs      |      🔜 Later | Build this after enough daily records exist.                                                             |


### Data Quality Status
| Data quality area     |  Status   | How                                                                                                        |
| --------------------- | --------: | ---------------------------------------------------------------------------------------------------------- |
| Missing values        |     ✅ Yes | `not_null` tests                                                                                           |
| Duplicates            |     ✅ Yes | `unique` tests and      `unique_stock_date` test                                                           |
| Relationships         |     ✅ Yes | Fact table foreign keys tested against dimensions                                                          |
| Accepted values       |     ✅ Yes | `price_movement_direction` only allows `up`, `down`, `unchanged`                                           |
| Business logic checks |     ✅ Yes | Checks like non-negative prices, high price greater than low price, and price position range               |
| Row counts            | 🟡 Partly | dbt docs/catalog shows row counts, but have not yet made row count a formal Airflow/dbt validation task     |


# Restart and Validation Flow

This guide shows the simple flow used to restart the local project and confirm that everything is working again.

Project:

```text
stock_market_finnhub
```

Current stack:

```text
Docker
Kafka
Kafdrop
Airflow
AWS S3
Snowflake
```

---

## 1. Start the project environment

Go to the project folder:

```bash
cd ~/de25/stock_market_finnhub
```

Activate the virtual environment:

```bash
source .venv_stock/Scripts/activate
```

Start the Docker containers:

```bash
docker compose up -d
```

OR when docker was stopped previosly

```bash
docker compose start
```

---

## 2. Check Docker containers

Run:

```bash
docker ps
```

Expected containers should be running:

```text
zookeeper
kafka
kafdrop
airflow-webserver
airflow-scheduler
airflow-postgres
```

If all containers show `Up`, Docker is working.

---

## 3. Check Kafka topic

Run:

```bash
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

Expected topic:

```text
stock_quotes
__consumer_offsets
```

If `stock_quotes` appears, Kafka is working.

If it is missing, create it again from Kafdrop UI or terminal.

Kafka topic settings:

```text
Topic name: stock_quotes
Partitions: 3
Replication factor: 1
```

---

## 4. Check Kafdrop UI

Open:

```text
http://localhost:9000
```

Expected result:

```text
Kafdrop opens successfully
stock_quotes topic is visible
```

If the topic is visible, Kafdrop can connect to Kafka.

---

## 5. Check Airflow UI

Open:

```text
http://localhost:8080
```

Expected DAGs:

```text
dag_smoke_test
test_snowflake_connection
```

Expected result:

```text
DAGs are visible
DAGs are healthy
Snowflake test DAG is green
```

If the Snowflake test DAG is green, Airflow can connect to Snowflake.

---

## 6. Check Snowflake raw table

In Snowflake, run:

```sql
USE WAREHOUSE finnhub_stocks_mds_wh;
USE DATABASE finnhub_stocks_mds;
USE SCHEMA raw;

SELECT COUNT(*) AS total_rows
FROM raw_stock_quotes;
```

Expected result:

```text
25 rows
```

Then run:

```sql
SELECT
    raw_record:symbol::STRING AS symbol,
    raw_record:current_price::FLOAT AS current_price,
    raw_record:change::FLOAT AS price_change,
    raw_record:percent_change::FLOAT AS percent_change,
    raw_record:fetched_at::TIMESTAMP_NTZ AS fetched_at,
    source_file_name,
    loaded_at
FROM raw_stock_quotes
LIMIT 5;
```

Expected result:

```text
Stock quote records are visible
Example symbols: AAPL, MSFT, TSLA, GOOGL, AMZN
```

If the rows appear, the Snowflake raw layer is still working.

---

## 7. Current confirmed status

After the restart check, these were confirmed working:

```text
Docker containers are running
Kafka is running
Kafka topic stock_quotes exists
Kafdrop UI is working
Airflow UI is working
Airflow DAGs are healthy
Airflow can connect to Snowflake
Snowflake raw_stock_quotes table still has 25 rows
Stock quote data from yesterday is still available
```
---

## 8. Check dbt version used

```bash
which dbt
```

Expect:
`/c/Users/adelo/de25/stock_market_finnhub/.venv_stock/Scripts/dbt`

If not correct:
```bash
cd ..
source .venv_stock/Scripts/activate
cd finnhub_stocks
which dbt
dbt --version
```

Expected:
```
/c/Users/adelo/de25/stock_market_finnhub/.venv_stock/Scripts/dbt
dbt-core 1.11.11
```