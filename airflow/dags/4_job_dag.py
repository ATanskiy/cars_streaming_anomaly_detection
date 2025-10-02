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
    '4_data_generator',
    default_args=default_args,
    description='Generate streaming data for car sales (start once, runs continuously)',
    schedule_interval=None,  # Manual trigger only - streaming job runs forever
    catchup=False,
    tags=['streaming', 'data-generation', 'long-running'],
)

generate_data = BashOperator(
    task_id='generate_sales_data',
    bash_command='docker exec spark spark-submit /opt/streaming/jobs/4_data_generator.py',
    dag=dag,
)