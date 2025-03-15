#!/bin/bash

# Initialize Conda environment
eval "$(conda shell.bash hook)"
conda activate iot_weather

# Copy DAG files to the Airflow DAGs directory
mkdir -p ~/airflow/dags
cp -f /home/nicholas/Documents/IOT_Weather/dags/*.py ~/airflow/dags/

# Function to check if a process is running and start it if not
start_if_not_running() {
    local process_name=$1
    local start_command=$2

    if pgrep -f "$process_name" > /dev/null; then
        echo "$process_name is already running."
    else
        echo "Starting $process_name..."
        eval "$start_command"
        echo "$process_name started."
    fi
}

# Start Airflow webserver if not running
start_if_not_running "airflow webserver" "airflow webserver --port 8080 -D"

# Start Airflow scheduler if not running
start_if_not_running "airflow scheduler" "airflow scheduler -D"

echo "Airflow is now running."
echo "- Webserver: http://localhost:8080"
echo "- Scheduler is running in the background"
echo "- Use 'ps aux | grep airflow' to see running processes"
echo "- Use 'kill <PID>' to stop individual processes"