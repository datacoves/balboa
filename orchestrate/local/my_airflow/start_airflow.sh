#!/bin/bash

# To reset psw use
# airflow users reset-password -u admin

# Load environment variables
export $(cat /config/workspace/orchestrate/local/my_airflow/.env | xargs)

# change location of __pycache__
export PYTHONPYCACHEPREFIX=/tmp

# Check if --airflow flag is present
if [[ "$*" == *"--airflow"* ]]; then
    echo "Starting Airflow standalone..."
    mkdir -p /config/airflow
    cp -r /config/workspace/orchestrate/local/my_airflow/airflow.cfg /config/airflow/
    uv run --no-project airflow standalone
else
    echo "Airflow flag not provided. Use --airflow to start Airflow."
    uv run --no-project echo "Environment variables are loaded and virtual environment is activated."
fi
