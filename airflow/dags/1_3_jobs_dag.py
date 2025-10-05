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
    '1_3_setup_tables',
    default_args=default_args,
    description='Create car models, colors, and sales tables',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['setup', 'tables'],
)

# Task 1: Create car models table
create_car_models = BashOperator(
    task_id='create_car_models',
    bash_command='docker exec spark spark-submit /opt/streaming/jobs/1_car_models.py',
    dag=dag,
)

# Task 2: Create car colors table
create_car_colors = BashOperator(
    task_id='create_car_colors',
    bash_command='docker exec spark spark-submit /opt/streaming/jobs/2_car_colors.py',
    dag=dag,
)

# Task 3: Create cars table
create_cars = BashOperator(
    task_id='create_cars',
    bash_command='docker exec spark spark-submit /opt/streaming/jobs/3_cars.py',
    dag=dag,
)

# Task 4: Create cars_raw table
create_cars_raw = BashOperator(
    task_id='create_cars_raw',
    bash_command='docker exec spark spark-submit /opt/streaming/jobs/3_1_cars_raw.py',
    dag=dag,
)

# Define task dependencies (chain them)
create_car_models >> create_car_colors >> create_cars >> create_cars_raw