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
}

dag = DAG(
    'stop_all_streaming_jobs',
    default_args=default_args,
    description='Stop all streaming jobs running in Spark container',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['streaming', 'management', 'stop'],
)

# Stop individual jobs
stop_data_generator = BashOperator(
    task_id='stop_data_generator',
    bash_command='docker exec spark pkill -f 4_data_generator.py || true',
    dag=dag,
)

stop_enriching = BashOperator(
    task_id='stop_enriching',
    bash_command='docker exec spark pkill -f 5_enriching.py || true',
    dag=dag,
)

stop_alerting = BashOperator(
    task_id='stop_alerting',
    bash_command='docker exec spark pkill -f 6_alerting.py || true',
    dag=dag,
)

stop_aggregations = BashOperator(
    task_id='stop_aggregations',
    bash_command='docker exec spark pkill -f 7_print_aggregations.py || true',
    dag=dag,
)