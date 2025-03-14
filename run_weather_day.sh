#!/bin/bash

# Log the start time
echo "Script started at $(date)" >> /home/nicholas/Documents/IOT_Weather/run_weather_day.log

# Initialize Conda
eval "$(conda shell.bash hook)"
conda activate iot_weather

# Run the weather_condition_day.py script
python3 /home/nicholas/Documents/IOT_Weather/weather_condition_day.py >> /home/nicholas/Documents/IOT_Weather/run_weather_day.log 2>&1

# Log the end time
echo "Script ended at $(date)" >> /home/nicholas/Documents/IOT_Weather/run_weather_day.log