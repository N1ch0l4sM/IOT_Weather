#!/bin/bash

# Log the start time
echo "Script started at $(date)" >> /home/nicholas/Documents/IOT_Weather/run_avg_month.log

# Initialize Conda
eval "$(conda shell.bash hook)"
conda activate iot_weather

# Run the avg_month.py script
python3 /home/nicholas/Documents/IOT_Weather/avg_month.py >> /home/nicholas/Documents/IOT_Weather/run_avg_month.log 2>&1

# Log the end time
echo "Script ended at $(date)" >> /home/nicholas/Documents/IOT_Weather/run_avg_month.log
