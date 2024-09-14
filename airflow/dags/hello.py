from airflow import DAG
from airflow.operators.bash_operator import BashOperator # type: ignore
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes = 5),
    # "queue": "bash_queue",
    # "pool": "backfill",
    # "priority_weight": 10,
    # "end_date": datetime(2016, 1, 1),
}

dag = DAG("main", default_args = default_args, schedule_interval = timedelta(30))

extract_load_task = BashOperator(
    task_id = "extract_load_task",
    bash_command = 'cd /opt/airflow/code && ./trino --server http://trino:8080', 
    dag = dag
)

extract_load_task