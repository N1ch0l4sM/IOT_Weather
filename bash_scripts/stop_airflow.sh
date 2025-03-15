#!/bin/bash

echo "Stopping Airflow services..."

# Stop the webserver
webserver_count=$(pgrep -f "airflow webserver" | wc -l)
if [ $webserver_count -gt 0 ]; then
    echo "Stopping Airflow webserver..."
    pkill -f "airflow webserver"
    echo "Webserver stopped."
else
    echo "Airflow webserver is not running."
fi

# Stop the scheduler
scheduler_count=$(pgrep -f "airflow scheduler" | wc -l)
if [ $scheduler_count -gt 0 ]; then
    echo "Stopping Airflow scheduler..."
    pkill -f "airflow scheduler"
    echo "Scheduler stopped."
else
    echo "Airflow scheduler is not running."
fi

echo "All Airflow services have been stopped."