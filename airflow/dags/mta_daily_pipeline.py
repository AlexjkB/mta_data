import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="mta_daily_pipeline",
    default_args=default_args,
    description="Daily MTA data pipeline: extract -> load -> transform",
    schedule="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mta", "pipeline"],
) as dag:

    # --- EXTRACT tasks (run in parallel) ---
    datasets = [
        "daily_ridership",
        "subway_hourly",
        "bus_hourly",
        "bridges_tunnels",
        "stations",
    ]

    extract_tasks = []
    for dataset in datasets:
        task = PythonOperator(
            task_id=f"extract_{dataset}",
            python_callable=lambda ds, **kwargs: __import__("pipelines.extract", fromlist=["extract_dataset"]).extract_dataset(
                ds, start_date=kwargs["ds"]
            ),
            op_args=[dataset],
        )
        extract_tasks.append(task)

    # --- LOAD task ---
    load_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=lambda: __import__("pipelines.load", fromlist=["load_all_datasets"]).load_all_datasets(),
    )

    # --- TRANSFORM tasks ---
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_mta && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_mta && dbt test --profiles-dir .",
    )

    # --- Dependencies ---
    extract_tasks >> load_task >> dbt_run >> dbt_test
