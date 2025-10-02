from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '7_print_aggregations',
    default_args=default_args,
    description='Print and compute aggregations on car sales data (start once, runs continuously)',
    schedule_interval=None,  # Manual trigger only - streaming job runs forever
    catchup=False,
    tags=['streaming', 'aggregations', 'long-running'],
)

aggregations_task = BashOperator(
    task_id='compute_aggregations',
    bash_command='docker exec spark spark-submit /opt/streaming/jobs/7_print_aggregations.py',
    dag=dag,
)