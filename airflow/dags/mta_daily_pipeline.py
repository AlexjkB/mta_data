from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

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

    with TaskGroup("extract") as extract_group:
        for dataset in datasets:
            BashOperator(
                task_id=f"extract_{dataset}",
                bash_command=f"cd /opt/airflow && python -m pipelines.extract --dataset {dataset} --start-date {{{{ ds }}}}",
            )

    # --- LOAD task ---
    with TaskGroup("load") as load_group:
        for dataset in datasets:
            BashOperator(
                task_id=f"load_{dataset}",
                bash_command=f"cd /opt/airflow && python -m pipelines.load --dataset {dataset}",
            )

    # --- TRANSFORM tasks ---
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_mta && dbt run --profiles-dir . --log-path /tmp/dbt_logs --target-path /tmp/dbt_target",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_mta && dbt test --profiles-dir . --log-path /tmp/dbt_logs --target-path /tmp/dbt_target",
    )

    # --- Dependencies ---
    extract_group >> load_group >> dbt_run >> dbt_test
