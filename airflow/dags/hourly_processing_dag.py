from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Hourly processing DAG
with DAG(
    'hourly_processing',
    default_args=default_args,
    description='Process hourly weather data',
    schedule_interval='5 * * * *',  # 5 minutes past every hour (after data collection)
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    project_dir = '/home/nicholas/Documents/IOT_Weather'
    conda_init = 'eval "$(conda shell.bash hook)" && conda activate iot_weather'
    
    # Hourly weather processing task
    weather_hour_task = BashOperator(
        task_id='weather_hour',
        bash_command=f'{conda_init} && python {project_dir}/scripts/weather_hour.py > {project_dir}/airflow_logs/airflow_weather_hour.log 2>&1',
    )