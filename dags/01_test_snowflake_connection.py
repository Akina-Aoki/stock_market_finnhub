import os
from datetime import datetime

import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator


def test_snowflake_connection():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    cur = conn.cursor()

    cur.execute("""
        SELECT
            CURRENT_USER(),
            CURRENT_ROLE(),
            CURRENT_WAREHOUSE(),
            CURRENT_DATABASE(),
            CURRENT_SCHEMA()
    """)

    result = cur.fetchone()
    print("Snowflake connection result:")
    print(result)

    cur.close()
    conn.close()

    print("Airflow Snowflake connection successful.")


with DAG(
    dag_id="test_snowflake_connection",
    start_date=datetime(2026, 6, 29),
    schedule_interval=None,
    catchup=False,
    tags=["snowflake", "test"],
) as dag:

    test_connection = PythonOperator(
        task_id="test_snowflake_connection",
        python_callable=test_snowflake_connection,
    )