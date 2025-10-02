
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    '8_iceberg_compaction',
    default_args=default_args,
    description='Compact Iceberg tables to optimize storage and query performance',
    schedule_interval='0 */6 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['maintenance', 'iceberg', 'compaction'],
)

compact_tables = BashOperator(
    task_id='compact_iceberg_tables',
    bash_command='docker exec spark spark-submit /opt/streaming/jobs/8_table_compaction.py',
    dag=dag,
)