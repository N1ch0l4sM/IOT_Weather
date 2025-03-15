from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import sys, os

# Add project directory to path so we can import modules
sys.path.append('/home/nicholas/Documents/IOT_Weather')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG for weather data collection (every hour)
with DAG(
    'weather_fetch',
    default_args=default_args,
    description='Fetch weather data from API and store in MongoDB',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    # Set up conda environment and project directory
    conda_init = 'eval "$(conda shell.bash hook)" && conda activate iot_weather'
    project_dir = '/home/nicholas/Documents/IOT_Weather'
    
    # Replacing the PythonOperator with BashOperator to run the weather fetch script
    fetch_task = BashOperator(
        task_id='fetch_weather_data',
        bash_command=f'{conda_init} && python {project_dir}/scripts/fetch_weather.py >> {project_dir}/airflow_logs/airflow_weather_fetch.log 2>&1',
    )