
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from configs.constants import TABLES_TO_COMPACT

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
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['maintenance', 'iceberg', 'compaction'],
)

previous_task = None
for schema_name, table_name in TABLES_TO_COMPACT.items():
    task_id = f'compact_{schema_name}_{table_name}'
    
    task = BashOperator(
        task_id=task_id,
        bash_command=f'docker exec spark spark-submit /opt/streaming/jobs/8_table_compaction.py {schema_name} {table_name}',
        dag=dag,
    )

    if previous_task:
        previous_task >> task
    previous_task = task