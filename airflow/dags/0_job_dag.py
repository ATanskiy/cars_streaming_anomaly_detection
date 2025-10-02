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
    '0_create_schema_with_trino',
    default_args=default_args,
    description='Create Iceberg schemas using Trino CLI',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['setup', 'schema', 'trino'],
)

# Assuming your Trino container is named 'trino' or 'trino-coordinator'
# Adjust the container name based on your docker-compose.yml

create_dims_schema = BashOperator(
    task_id='create_dims_schema',
    bash_command="""
        docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.dims WITH (location = 's3a://spark/data/dims')"
    """,
    dag=dag,
)

create_dims_schema