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
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': None,
}

dag = DAG(
    '5_enriching_stream',
    default_args=default_args,
    description='Enrich streaming car sales data (start once, runs continuously)',
    schedule_interval=None,  # Manual trigger only - streaming job runs forever
    catchup=False,
    tags=['streaming', 'enrichment', 'long-running'],
)

enrich_data = BashOperator(
    task_id='enrich_sales_data',
    bash_command='docker exec spark spark-submit /opt/streaming/jobs/5_enriching.py',
    dag=dag,
)