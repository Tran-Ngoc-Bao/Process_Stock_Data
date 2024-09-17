from airflow import DAG
from airflow.operators.bash_operator import BashOperator # type: ignore
from airflow.operators.python_operator import PythonOperator # type: ignore
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

def extract_load():
    print(1)

extract_load_task = PythonOperator(
    task_id = "extract_load_task",
    python_callable = extract_load, 
    dag = dag
)

transform_task = BashOperator(
    task_id = "transform_task",
    bash_command = 'spark-submit /opt/airflow/code/spark.py', 
    dag = dag
)

query_task = BashOperator(
    task_id = "query_task",
    bash_command = 'cd /opt/airflow/code && ./trino --server http://trino:8080 --file trino.sql', 
    dag = dag
)

extract_load_task >> transform_task >> query_task