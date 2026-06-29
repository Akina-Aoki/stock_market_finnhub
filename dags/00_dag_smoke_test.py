from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def hello_airflow():
    print("Airflow can read DAG files from the local dags folder.")


with DAG(
    dag_id="dag_smoke_test",
    start_date=datetime(2026, 6, 29),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
) as dag:

    test_task = PythonOperator(
        task_id="hello_airflow",
        python_callable=hello_airflow,
    )