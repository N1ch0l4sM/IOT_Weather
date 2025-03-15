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

# Monthly aggregation DAG (runs on first day of each month)
with DAG(
    'monthly_aggregation',
    default_args=default_args,
    description='Process monthly weather aggregations',
    schedule_interval='0 2 1 * *',  # 2:00 AM on the 1st day of each month
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    project_dir = '/home/nicholas/Documents/IOT_Weather'
    conda_init = 'eval "$(conda shell.bash hook)" && conda activate iot_weather'
    
    # Monthly aggregation task
    month_task = BashOperator(
        task_id='avg_month',
        bash_command=f'{conda_init} && python {project_dir}/scripts/avg_month.py > {project_dir}/airflow_logs/airflow_avg_month.log 2>&1',
    )