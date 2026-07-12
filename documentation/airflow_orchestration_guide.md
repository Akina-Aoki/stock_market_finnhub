# Airflow Orchestration (Show dag_flow chart in README)
> I used Airflow to orchestrate the full pipeline. Each DAG run creates a unique run_id, runs a one-shot producer to fetch 5 Finnhub stock quotes, sends them to Kafka, runs a one-shot consumer to upload the batch to S3, loads the JSONL file into Snowflake RAW, checks the raw batch quality, runs dbt transformations, runs dbt tests, and finally validates that the mart fact table has rows for the current batch. This proves the pipeline works end-to-end from API ingestion to analytics-ready tables.
### The Airflow workflow is:

```text
Finnhub API
→ Kafka
→ S3
→ Snowflake RAW
→ dbt transformations
→ dbt tests
→ MARTS validation
```

Airflow is the control center of the pipeline. It runs each step in the correct order, from ingestion to transformation, testing, and final mart validation.

It makes the project more production-like because it controls the full pipeline instead of relying on me to manually run each script.



## ✅ `run_id`

Each Airflow DAG run creates a unique `run_id`.

The producer adds this `run_id` to every Kafka message.

The consumer only reads messages with the same `run_id`.

The run_id prevents old Kafka messages from being mixed into a new pipeline run. This makes the pipeline safer and easier to debug.


## ✅* Producer task

The producer script is:

```text
producer/producer_once.py
```

Its job is to:

```text
1. Fetch stock quotes from Finnhub
2. Add the current run_id
3. Send 5 messages to Kafka
4. Exit
```

The stock symbols are:

```text
AAPL
MSFT
TSLA
GOOGL
AMZN
```

## ✅* Consumer task

The consumer script is:

```text
consumer/consumer_once.py
```

Its job is to:

```text
1. Read messages from Kafka
2. Keep only messages for the current run_id
3. Collect 5 matching messages
4. Save them as one JSONL file in S3
5. Commit Kafka offsets after successful S3 upload
6. Exit
```


## Docker and Airflow setup

Airflow runs inside Docker, so it needs access to the project files and dependencies.

Important mounted folders:

```text
producer/
consumer/
finnhub_stocks/
sql/
```

Important Python packages added to Airflow:

```text
kafka-python
boto3
snowflake-connector-python
dbt-core
dbt-snowflake
python-dotenv
requests
```

> Since Airflow runs inside Docker, I had to make sure the Airflow container had the right folders, Python packages, environment variables, and dbt profile.

---

## dbt inside Airflow

dbt worked locally, but Airflow also needed to run dbt inside the Docker container.

So I used:

```text
DBT_PROFILES_DIR=/opt/airflow/finnhub_stocks
```

The dbt profile reads Snowflake credentials from environment variables.


> I made dbt work inside the Airflow container by giving Airflow access to the dbt project, the dbt profile, and the Snowflake environment variables. Secrets are not hardcoded in the dbt profile.


## ✅ S3 to Snowflake RAW

After the consumer uploads the JSONL file to S3, Airflow loads the file into Snowflake.

This uses Snowflake `COPY INTO`.

Simple flow:

```text
S3 JSONL file
→ Snowflake external stage
→ COPY INTO
→ RAW_STOCK_QUOTES
```

The raw table is:

```text
FINNHUB_STOCKS_MDS.RAW.RAW_STOCK_QUOTES
```


> After the batch is saved in S3, Airflow loads it into the Snowflake RAW layer using COPY INTO. The raw records are stored as JSON in a VARIANT column.

---

## ✅ RAW quality check

Before dbt runs, Airflow checks that the raw batch is complete.

The RAW check validates:

```text
5 rows loaded
5 distinct symbols
no missing symbol
no missing current_price
no missing fetched_at
no duplicate symbols in the same run
```

> the pipeline fails early if the ingestion batch is incomplete or broken.

Difference:

```text
Airflow RAW check = checks the ingestion batch
dbt tests = check the transformed models
```


## ✅ dbt run and dbt test

After the RAW check passes, Airflow runs:

```text
dbt run
dbt test
```


> dbt run builds the staging, intermediate, and mart models. dbt test checks data quality in the transformed models.

Examples of dbt tests:

```text
not_null
unique
relationships
accepted values
custom SQL tests
```

---

##   ✅Final MARTS validation

After dbt finishes, Airflow checks that the current batch reached the final mart table:

```text
MARTS.FCT_STOCK_QUOTES_DAILY
```


## How I tested the DAG

I tested the important connections one by one:

```text
Airflow could see the DAG
No DAG import errors
dbt worked inside Airflow
Producer worked inside Airflow
Consumer worked inside Airflow
Snowflake COPY worked inside Airflow
Full DAG run succeeded
```

What to say:

> I did not test everything only at the end. I tested each connection separately first, then tested the full DAG run.

---

## ✅ Final proof

In the final Airflow run, all tasks were green:

```text
start                   success
create_run_id           success
run_producer_once       success
run_consumer_once       success
copy_s3_to_snowflake    success
check_raw_batch_quality success
dbt_run                 success
dbt_test                success
check_mart_row_count    success
end                     success
```


> This proves that the full orchestration works end-to-end.

---

##  ✅* Main lesson learned

The biggest lesson was that making the script work locally is not enough.

Airflow runs inside Docker, so the container also needs:

```text
right Python packages
right mounted folders
right environment variables
right Kafka address
right dbt profile
right Snowflake credentials
```


> The hardest part was not only writing the scripts, but making sure every tool could talk to the next tool inside the same Docker runtime environment.
