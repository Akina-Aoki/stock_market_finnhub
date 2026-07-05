"""Orchestrate the Finnhub daily stock quote ingestion pipeline.

This Airflow DAG coordinates one batch of stock quote data from the Finnhub
producer through Kafka, an S3 landing area, Snowflake RAW storage, dbt models,
dbt tests, and a final MARTS validation. The workflow is designed to make each
batch traceable with a shared ``run_id`` so data engineers can connect records
across ingestion, loading, transformation, and validation steps. 
"""


import os
from datetime import datetime, timezone, timedelta

import pendulum
import snowflake.connector
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

EXPECTED_RAW_ROW_COUNT = 5
SNOWFLAKE_RAW_SCHEMA = "RAW"


def create_run_id(**context) -> str:
    """Build the batch identifier used to trace this DAG run.

    The value is derived from Airflow's logical date when available, which keeps
    the identifier deterministic for a scheduled run. Downstream ingestion,
    loading, and validation tasks use the same value for batch lineage and for
    filtering Snowflake checks to only the records produced by this run.
    """
    
    logical_date = context.get("logical_date")

    if logical_date is None:
        timestamp = datetime.now(timezone.utc)
    else:
        timestamp = (
            logical_date.in_timezone("UTC")
            if hasattr(logical_date, "in_timezone")
            else logical_date
        )

    return f"airflow_{timestamp.strftime('%Y%m%dT%H%M%SZ')}"


def get_snowflake_connection(schema: str = SNOWFLAKE_RAW_SCHEMA):
    """Open a Snowflake connection using Airflow-provided .env settings.

    Keeping connection creation in one helper makes Snowflake access consistent
    across tasks and avoids duplicating credential, warehouse, database, and
    schema configuration in each validation or loading function.
    """
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=schema,
    )


def copy_s3_to_snowflake() -> None:
    """Load staged JSONL quote files from S3 into the Snowflake RAW layer.

    ``COPY INTO`` is Snowflake's bulk-loading pattern for moving files from an
    external stage into a table. This task preserves the original JSON payload in
    ``raw_record`` and stores the source file name so the raw layer remains an
    auditable, minimally transformed record of what landed from ingestion.
    """
    copy_sql = """
        COPY INTO raw_stock_quotes (raw_record, source_file_name)
        FROM (
            SELECT
                $1 AS raw_record,
                METADATA$FILENAME AS source_file_name
            FROM @finnhub_stock_quotes_stage
        )
        FILE_FORMAT = (FORMAT_NAME = jsonl_file_format)
        ON_ERROR = 'CONTINUE';
    """

    with get_snowflake_connection(schema=SNOWFLAKE_RAW_SCHEMA) as conn:
        with conn.cursor() as cur:
            cur.execute(copy_sql)
            print("Snowflake COPY INTO completed.")


def check_raw_batch_quality(**context) -> None:
    """Validate that the current raw batch is complete and usable for dbt.

    The check is scoped to the shared ``run_id`` so it evaluates only this DAG
    run's batch. It verifies expected row volume, symbol coverage, required JSON
    fields, and duplicate symbols before allowing downstream dbt models to build
    curated analytics tables from the RAW layer.
    """
    # XCom carries the run_id from the batch creation task to validation tasks
    # without hard-coding dates, preserving lineage across the orchestrated run.
    run_id = context["ti"].xcom_pull(task_ids="create_run_id")
    if not run_id:
        raise AirflowException("Missing run_id from create_run_id XCom.")

    quality_query = """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT raw_record:symbol::STRING) AS distinct_symbols,
            SUM(CASE WHEN raw_record:symbol::STRING IS NULL THEN 1 ELSE 0 END) AS missing_symbols,
            SUM(CASE WHEN raw_record:current_price::FLOAT IS NULL THEN 1 ELSE 0 END) AS missing_current_prices,
            SUM(CASE WHEN raw_record:fetched_at::STRING IS NULL THEN 1 ELSE 0 END) AS missing_fetched_at
        FROM raw_stock_quotes
        WHERE raw_record:run_id::STRING = %s;
    """
    duplicate_symbols_query = """
        SELECT
            raw_record:symbol::STRING AS symbol,
            COUNT(*) AS symbol_count
        FROM raw_stock_quotes
        WHERE raw_record:run_id::STRING = %s
        GROUP BY raw_record:symbol::STRING
        HAVING COUNT(*) > 1;
    """

    with get_snowflake_connection(schema=SNOWFLAKE_RAW_SCHEMA) as conn:
        with conn.cursor() as cur:
            cur.execute(quality_query, (run_id,))
            (
                row_count,
                distinct_symbols,
                missing_symbols,
                missing_current_prices,
                missing_fetched_at,
            ) = cur.fetchone()

            cur.execute(duplicate_symbols_query, (run_id,))
            duplicate_symbols = cur.fetchall()

    missing_symbols = missing_symbols or 0
    missing_current_prices = missing_current_prices or 0
    missing_fetched_at = missing_fetched_at or 0

    print(
        "RAW batch quality for "
        f"run_id={run_id}: row_count={row_count}, "
        f"distinct_symbols={distinct_symbols}, missing_symbols={missing_symbols}, "
        f"missing_current_prices={missing_current_prices}, "
        f"missing_fetched_at={missing_fetched_at}, "
        f"duplicate_symbols={duplicate_symbols}"
    )

    # RAW quality validation catches incomplete, malformed, or duplicated input
    # before transformation so bad source data does not propagate to marts.
    quality_errors = []
    if row_count != EXPECTED_RAW_ROW_COUNT:
        quality_errors.append(
            f"expected {EXPECTED_RAW_ROW_COUNT} rows, found {row_count}"
        )
    if distinct_symbols != EXPECTED_RAW_ROW_COUNT:
        quality_errors.append(
            f"expected {EXPECTED_RAW_ROW_COUNT} distinct symbols, found {distinct_symbols}"
        )
    if missing_symbols > 0:
        quality_errors.append(f"found {missing_symbols} missing symbols")
    if missing_current_prices > 0:
        quality_errors.append(
            f"found {missing_current_prices} missing current_price values"
        )
    if missing_fetched_at > 0:
        quality_errors.append(f"found {missing_fetched_at} missing fetched_at values")
    if duplicate_symbols:
        duplicate_details = ", ".join(
            f"{symbol or '<NULL>'}={symbol_count}"
            for symbol, symbol_count in duplicate_symbols
        )
        quality_errors.append(f"found duplicate symbols: {duplicate_details}")

    if quality_errors:
        raise AirflowException(
            f"RAW batch quality failed for run_id={run_id}: "
            + "; ".join(quality_errors)
        )


def check_mart_row_count(**context) -> None:
    """Confirm the final mart contains analytics rows for this batch date.

    The check derives the batch's fetched date from RAW records and then confirms
    the dbt-built fact table has rows for that date. This validates that data was
    not only ingested, but also transformed into the MARTS layer where analysts
    and dashboards are expected to query it.
    """
    # The same XCom-provided run_id links mart validation back to the exact raw
    # batch, keeping the final analytics check tied to upstream lineage.
    run_id = context["ti"].xcom_pull(task_ids="create_run_id")
    if not run_id:
        raise AirflowException("Missing run_id from create_run_id XCom.")

    fetched_date_query = """
        SELECT CAST(MIN(raw_record:fetched_at::TIMESTAMP_NTZ) AS DATE)
        FROM RAW.raw_stock_quotes
        WHERE raw_record:run_id::STRING = %s;
    """
    mart_count_query = """
        SELECT COUNT(*)
        FROM MARTS.fct_stock_quotes_daily fact
        INNER JOIN MARTS.dim_date dim
            ON fact.date_id = dim.date_id
        WHERE dim.calendar_date = %s;
    """

    with get_snowflake_connection(schema=SNOWFLAKE_RAW_SCHEMA) as conn:
        with conn.cursor() as cur:
            cur.execute(fetched_date_query, (run_id,))
            fetched_date = cur.fetchone()[0]

            if fetched_date is None:
                raise AirflowException(f"No fetched_date found in RAW for run_id={run_id}.")

            cur.execute(mart_count_query, (fetched_date,))
            row_count = cur.fetchone()[0]

    print(f"MARTS fact row count for fetched_date={fetched_date}: {row_count}")
    if row_count == 0:
        raise AirflowException(
            "Expected MARTS.fct_stock_quotes_daily rows for "
            f"run_id={run_id}, fetched_date={fetched_date}, found 0."
        )


with DAG(
    dag_id="finnhub_daily_stock_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Stockholm"),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["finnhub", "kafka", "s3", "snowflake", "dbt"],
) as dag:
    # Pipeline start: gives the DAG a clear entry point in the Airflow graph.
    start = EmptyOperator(task_id="start")

    # Batch ID creation: one run_id is shared by all tasks for lineage,
    # idempotency, and scoped validation of this scheduled batch.
    create_run_id_task = PythonOperator(
        task_id="create_run_id",
        python_callable=create_run_id,
    )

    # Ingestion tasks: Airflow passes run_id through XCom templating into the
    # producer/consumer environment so Kafka and S3 records keep batch context.
    run_producer = BashOperator(
        task_id="run_producer_once",
        bash_command="python /opt/airflow/producer/producer_once.py",
        env={"RUN_ID": "{{ ti.xcom_pull(task_ids='create_run_id') }}"},
        append_env=True,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )

    # Snowflake loading: COPY INTO moves staged S3 files into the RAW layer,
    # keeping ingestion data available for replay, auditing, and transformation.
    run_consumer = BashOperator(
        task_id="run_consumer_once",
        bash_command="python /opt/airflow/consumer/consumer_once.py",
        env={"RUN_ID": "{{ ti.xcom_pull(task_ids='create_run_id') }}"},
        append_env=True,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )

    # Quality checks: validate the raw batch before dbt builds curated models.
    copy_s3_to_snowflake_task = PythonOperator(
        task_id="copy_s3_to_snowflake",
        python_callable=copy_s3_to_snowflake,
    )

    # dbt transformation/testing: dbt run builds the models first; dbt test then
    # checks the freshly built outputs for expected constraints and assumptions.
    check_raw_batch_quality_task = PythonOperator(
        task_id="check_raw_batch_quality",
        python_callable=check_raw_batch_quality,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/finnhub_stocks && dbt run",
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/finnhub_stocks && dbt test",
        append_env=True,
    )

    # Mart validation: confirm the final analytics layer has rows for this batch
    # date, proving the data reached the table used by downstream consumers.
    check_mart_row_count_task = PythonOperator(
        task_id="check_mart_row_count",
        python_callable=check_mart_row_count,
    )

    # Pipeline end: marks successful completion after all validation has passed.
    end = EmptyOperator(task_id="end")

    (
        start
        >> create_run_id_task
        >> run_producer
        >> run_consumer
        >> copy_s3_to_snowflake_task
        >> check_raw_batch_quality_task
        >> dbt_run
        >> dbt_test
        >> check_mart_row_count_task
        >> end
    )