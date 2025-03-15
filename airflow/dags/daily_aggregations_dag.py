from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG for daily aggregations (runs once a day)
with DAG(
    'daily_aggregations',
    default_args=default_args,
    description='Process daily weather aggregations',
    schedule_interval='30 0 * * *',  # Every day at 0:30 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    # Sensor to wait for weather_fetch dag to finish successfully
    wait_for_weather_fetch = ExternalTaskSensor(
        task_id='wait_for_weather_fetch',
        external_dag_id='weather_fetch',
        external_task_id=None,  # waits for the entire DAG
        allowed_states=['success'],
        poke_interval=60,
        timeout=600,
        mode='reschedule'
    )
    
    project_dir = '/home/nicholas/Documents/IOT_Weather'
    conda_init = 'eval "$(conda shell.bash hook)" && conda activate iot_weather'
    
    # Temperature aggregation
    temp_task = BashOperator(
        task_id='avg_temp_day',
        bash_command=f'{conda_init} && python {project_dir}/scripts/avg_temp_day.py > {project_dir}/airflow_logs/airflow_avg_temp.log 2>&1',
    )
    
    # Humidity aggregation
    humidity_task = BashOperator(
        task_id='avg_humidity_day',
        bash_command=f'{conda_init} && python {project_dir}/scripts/avg_humidity_day.py > {project_dir}/airflow_logs/airflow_avg_humidity.log 2>&1',
    )
    
    # Wind aggregation
    wind_task = BashOperator(
        task_id='avg_wind_day',
        bash_command=f'{conda_init} && python {project_dir}/scripts/avg_wind_day.py > {project_dir}/airflow_logs/airflow_avg_wind.log 2>&1',
    )
    
    # Rain aggregation
    rain_task = BashOperator(
        task_id='avg_rain_day',
        bash_command=f'{conda_init} && python {project_dir}/scripts/avg_rain_day.py > {project_dir}/airflow_logs/airflow_avg_rain.log 2>&1',
    )
    
    # Pressure aggregation
    pressure_task = BashOperator(
        task_id='avg_pressure_day',
        bash_command=f'{conda_init} && python {project_dir}/scripts/avg_pressure_day.py > {project_dir}/airflow_logs/airflow_avg_pressure.log 2>&1',
    )
    
    # Weather condition aggregation
    weather_condition_task = BashOperator(
        task_id='weather_condition_day',
        bash_command=f'{conda_init} && python {project_dir}/scripts/weather_condition_day.py > {project_dir}/airflow_logs/airflow_weather_condition.log 2>&1',
    )

    # Set dependencies: all tasks start after the weather_fetch DAG finishes
    wait_for_weather_fetch >> [temp_task, humidity_task, wind_task, rain_task, pressure_task, weather_condition_task]