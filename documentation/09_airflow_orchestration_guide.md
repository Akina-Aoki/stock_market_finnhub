# Airflow Orchestration: Finnhub Pipeline
This document explains how I built and tested the Airflow DAG for my Finnhub modern data stack project.

The goal was to move from a manual pipeline into an orchestrated pipeline that can run automatically. The final workflow is:
```
Finnhub API
→ Kafka
→ S3
→ Snowflake RAW
→ dbt staging/intermediate/marts
→ dbt tests
→ mart validation
```

The main thing I changed was how Kafka is used. At first, the producer and consumer were written more like streaming scripts that could keep running. That was useful for learning Kafka, but it was not ideal for Airflow because an Airflow task should usually start, do one clear job, and then finish.

So I created one-shot versions of the producer and consumer:
```
producer/producer_once.py
consumer/consumer_once.py
```

These scripts run once, process one batch, and then exit. This makes them much easier to orchestrate with Airflow.

## 1. Why I changed the Kafka design from real-time to batch

In the beginning, Kafka was working more like a continuous streaming setup. The producer could keep sending messages and the consumer could keep listening.

That is a valid pattern in real-time systems, but for this project I wanted a daily batch-style pipeline:

Once per scheduled run:
1. Fetch stock quotes from Finnhub
2. Send them to Kafka
3. Consume the same batch from Kafka
4. Store the batch to S3
5. Load it into Snowflake
6. Transform and test it with dbt

So instead of having Kafka run endlessly, I made the pipeline batch-controlled.

The important idea is Kafka is still part of the pipeline,
but Airflow controls when the Kafka producer and consumer run.

## 2. The role of RUN_ID

To connect the producer and consumer safely, I added a `RUN_ID.`

Each Airflow DAG run creates one unique `run_id`, for example:

`airflow_20260703T094646Z`

The producer attaches this run_id to every Kafka message.

Example:
```json

{
  "run_id": "airflow_20260703T094646Z",
  "symbol": "AAPL",
  "current_price": 213.55,
  "fetched_at": "2026-07-03T09:48:36Z"
}
```

The consumer then only accepts messages with the same `run_id`.

**This is important because Kafka can still contain old messages from earlier test runs. Without a run_id, the consumer could accidentally consume old data.**

So the rule became:

Producer creates messages with the current `run_id`.
Consumer only uploads messages that match the current run_id.

This made the pipeline safer and easier to debug.

## 3. Producer: Finnhub API to Kafka

The producer script is:

`producer/producer_once.py`

Its job is to:

1. Read the Finnhub API key from environment variables
2. Fetch quotes for 5 stock symbols
3. Add the current `run_id` to each record
4. Send the records to the Kafka topic stock_quotes
5. Exit

The symbols are:
```
AAPL
MSFT
TSLA
GOOGL
AMZN
```

The producer sends 5 Kafka messages per run.

Locally, Kafka is reached through:

`localhost:29092`

**Inside Docker/Airflow, Kafka is reached through:**

`kafka:9092`

This difference matters because Airflow runs inside Docker. From inside the Docker network, localhost means the Airflow container itself, not the Kafka container.

So in `docker-compose.yml`, I set:

`KAFKA_BOOTSTRAP_SERVERS=kafka:9092`

This allowed the Airflow container to talk to Kafka correctly.


## 4. Consumer: Kafka to S3

The consumer script is:

`consumer/consumer_once.py`

Its job is to:

1. Connect to Kafka
2. Read messages from the stock_quotes topic
3. Keep only messages with the current `run_id`
4. Stop after it has collected 5 matching messages
5. Write those 5 records as one JSONL file to S3
6. Commit Kafka offsets only after a successful S3 upload
7. Exit

The S3 path looks like this:

```
s3://finnhub-stocks/raw/stock_quotes/ingestion_date=2026-07-03/hour=09/batch_<run_id>_<timestamp>.jsonl
```

This gives the raw data a clear folder structure by date and hour.

The file format is JSONL, meaning one JSON record per line. This works well with Snowflake `COPY INTO.`


## 5. Docker Compose changes

To make Airflow run the full project, I had to update `docker-compose.yml`.

The Airflow containers needed access to my local project folders, so I mounted them into the containers:

```
- ./producer:/opt/airflow/producer
- ./consumer:/opt/airflow/consumer
- ./finnhub_stocks:/opt/airflow/finnhub_stocks
- ./sql:/opt/airflow/sql
```

This means Airflow can run:

```
/opt/airflow/producer/producer_once.py
/opt/airflow/consumer/consumer_once.py
/opt/airflow/finnhub_stocks/dbt_project.yml
```

I also added the required Python packages to Airflow using:
```
_PIP_ADDITIONAL_REQUIREMENTS=kafka-python boto3 snowflake-connector-python dbt-core dbt-snowflake python-dotenv requests
```


**These packages are needed because Airflow runs inside its own container. Even though the packages exist in my local virtual environment, Airflow cannot use them unless they are installed inside the Airflow container too.**

## 6. dbt inside Airflow

**dbt worked locally, but Airflow needed its own dbt profile.**

Locally, dbt uses:

```
...\.dbt\profiles.yml`
```

Inside Docker, Airflow looked for:
```
/home/airflow/.dbt/profiles.yml
```

So I created a project-level profile:

```
finnhub_stocks/profiles.yml
```

Then I told Airflow where to find it by adding:

```
DBT_PROFILES_DIR=/opt/airflow/finnhub_stocks
```

**The profile does not hardcode secrets. It reads Snowflake credentials from environment variables:**

```
account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
user: "{{ env_var('SNOWFLAKE_USER') }}"
password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
```

This is better because credentials stay in `.env`, not in the committed project code.

### I tested dbt inside the Airflow container with:
```
docker exec -it airflow-scheduler bash -c "cd /opt/airflow/finnhub_stocks && dbt debug"
```

The important result was:
```
Connection test: [OK connection ok]
```

Then I tested:
```
docker exec -it airflow-scheduler bash -c "cd /opt/airflow/finnhub_stocks && dbt build --select marts"
```

This completed successfully, which proved:

**Airflow container → dbt → Snowflake** was working.

## 7. S3 to Snowflake RAW

After the consumer uploaded a JSONL file to S3, the next step was loading it into Snowflake.

The DAG uses Snowflake `COPY INTO`:

```
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

This loads JSONL records from the S3 external stage into the Snowflake RAW table:
```
RAW.raw_stock_quotes
```

Each record is stored as a `VARIANT` column called:

`raw_record`


I manually tested this from inside the Airflow container before putting it into the DAG. The test loaded 5 rows successfully:

```
5 rows parsed
5 rows loaded
0 errors
```

That proved:

**Airflow container → S3 stage → Snowflake RAW** was working.

## 8. RAW batch quality check

I added a `RAW` quality check in the DAG before running dbt.

This check makes sure the current batch is complete and clean enough to transform.

It checks:

1. Exactly 5 rows were loaded for the current `run_id`
2. There are exactly 5 distinct symbols
3. No missing `symbol` values
4. No missing `current_price` values
5. No missing `fetched_at` values
6. No duplicate symbols within the same `run_id`

This check is done at the `RAW` layer because I want to catch obvious ingestion problems early.

**For example, if the API only returned 4 symbols, or Kafka only consumed part of the batch, the DAG should fail before dbt runs.**

This is different from dbt tests. **The RAW check validates the batch load itself, while dbt tests validate the transformed models.**

## 9. dbt run and dbt test

After the RAW data passes the batch quality check, the DAG runs:
```
dbt run
```

This builds the dbt models:

```
staging
intermediate
marts
```

Then the DAG runs:

```
dbt test
```

The dbt tests cover staging-layer quality, including:

```
not_null tests
unique tests
relationship tests
accepted values
custom SQL tests
```

Examples of what dbt tests check:

- `stock_symbol_id` is not null and unique
- `date_id` is not null and unique
- fact table foreign keys match dimension tables
- `high_price` is greater than or equal to `low_price`
- prices are non-negative
- one stock symbol/date combination appears only once in the fact table

**So the project has data quality checks in two places:**
```
Airflow RAW check = batch ingestion quality
dbt tests         = transformed model quality
```

## 10. MARTS validation

After `dbt run` and `dbt test`, the **DAG** does one final validation.

It finds the `fetched_date` for the current `run_id` in the `RAW` table.

Then it checks that the mart fact table has rows for that date:

```
MARTS.fct_stock_quotes_daily
```

joined with:

```
MARTS.dim_date
```

**This makes sure the current batch actually reached the marts layer. So the DAG does not only check that RAW was loaded. It also checks that the final mart layer was populated.**

## 11. Final Airflow DAG flow

The final DAG is:

`finnhub_daily_stock_pipeline`

It runs on this schedule: `23:00, Monday to Friday`
In cron format: `0 23 * * 1-5`

The tasks are:

```
start
→ create_run_id
→ run_producer_once
→ run_consumer_once
→ copy_s3_to_snowflake
→ check_raw_batch_quality
→ dbt_run
→ dbt_test
→ check_mart_row_count
→ end
```

I also added:

`max_active_runs=1`

**This prevents two pipeline runs from running at the same time.**

That matters because this pipeline uses Kafka messages, S3 files, Snowflake loads, and dbt table builds. Running two DAG runs at once could make debugging harder or cause overlapping batches.

## 12. How I tested the DAG

Before trusting the DAG, I tested each bridge separately.

### Airflow could see the DAG
```
docker exec -it airflow-scheduler bash -c "airflow dags list | grep finnhub"
```

Result:
```
finnhub_daily_stock_pipeline
```

### No DAG import errors


Test:
```
docker exec -it airflow-scheduler bash -c "airflow dags list-import-errors"
```

Result:
```
No data found
```

### dbt worked inside Airflow

Test:
```
docker exec -it airflow-scheduler bash -c "cd /opt/airflow/finnhub_stocks && dbt build --select marts"
```

Result:
```
Completed successfully
```

### Producer worked inside Airflow

Test:
```
docker exec -e RUN_ID="$RUN_ID" -it airflow-scheduler bash -c "python /opt/airflow/producer/producer_once.py"
```

This fetched 5 stock quotes and sent 5 messages to Kafka.


### Consumer worked inside Airflow
```
docker exec -e RUN_ID="$RUN_ID" -it airflow-scheduler bash -c "python /opt/airflow/consumer/consumer_once.py"
```

This consumed 5 matching messages and uploaded one JSONL file to S3.


### Snowflake COPY worked inside Airflow

The Airflow container successfully loaded the S3 JSONL file into Snowflake RAW.

The result showed:
```
5 rows loaded
0 errors
Full DAG run succeeded
```

### After triggering the DAG manually, all tasks succeeded:
```
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
**This proved the full orchestration works end-to-end.**


## 13. Lessons learned

I started with Kafka producer and consumer scripts that were closer to continuous streaming. That was good for learning Kafka, but not ideal for Airflow because Airflow tasks should finish. So I refactored them into one-shot scripts. Each DAG run creates a unique run_id. The producer attaches that run_id to each Kafka message, and the consumer only processes messages for that same run_id. This prevents old Kafka messages from being mixed into a new batch.

Airflow then orchestrates the whole pipeline. It runs the producer, runs the consumer, loads the S3 JSONL file into Snowflake RAW using COPY INTO, checks the RAW batch quality, runs dbt transformations, runs dbt tests, and finally checks that the mart fact table has rows for the batch date.

I also tested every connection from inside the Airflow Docker container, because the DAG runs inside Docker, not from my local virtual environment. I verified Kafka, S3, Snowflake, and dbt one by one before running the full DAG. Finally, I added max_active_runs=1 to avoid overlapping pipeline runs.

---

The biggest lesson from this part was that building the script is only one part of the work. The harder part is making sure every tool can talk to the next one inside the same runtime environment.

Locally, something can work because my virtual environment has the right packages. But Airflow runs inside Docker, so I had to make sure the Airflow container had:

```
the right Python packages
the right mounted folders
the right environment variables
the right Kafka address
the right dbt profile
the right Snowflake credentials
```

Once those pieces were aligned, the DAG could run the pipeline end-to-end.

**This part made the project feel much more like a real data engineering workflow, because Airflow became the control center for the whole pipeline.**