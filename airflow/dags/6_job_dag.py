from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': None,
}

dag = DAG(
    '6_alerting_stream',
    default_args=default_args,
    description='Monitor and alert on streaming anomalies (start once, runs continuously)',
    schedule_interval=None,  # Manual trigger only - streaming job runs forever
    catchup=False,
    tags=['streaming', 'alerting', 'long-running'],
)

alert_task = BashOperator(
    task_id='check_anomalies',
    bash_command='docker exec spark spark-submit /opt/streaming/jobs/6_alerting.py',
    dag=dag,
)