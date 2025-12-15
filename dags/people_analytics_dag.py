from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

with DAG(
    dag_id="people_analytics_etl",
    default_args=default_args,
    start_date=datetime(2025, 12, 9),
    schedule_interval=None,  # manual trigger for now
    catchup=False,
    tags=["people", "etl"],
) as dag:

    transform_employee = BashOperator(
        task_id="transform_employee",
        bash_command="python etl/transform/transform_employee.py",
        cwd="/opt/airflow"
    )

    transform_master = BashOperator(
        task_id="transform_master",
        bash_command="python etl/transform/transform_master.py",
        cwd="/opt/airflow"
    )

    load_bigquery = BashOperator(
        task_id="load_bigquery",
        bash_command="python etl/load/load_to_bigquery.py",
        cwd="/opt/airflow"
    )

    transform_employee >> transform_master >> load_bigquery
