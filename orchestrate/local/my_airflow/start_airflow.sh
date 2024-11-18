#!/bin/bash

# Activate the virtual environment
source /config/workspace/orchestrate/local/my_airflow/.venv/bin/activate

# Load environment variables
export $(cat /config/workspace/orchestrate/local/my_airflow/.env | xargs)

# change location of __pycache__
export PYTHONPYCACHEPREFIX=/tmp

# Check if --airflow flag is present
if [[ "$*" == *"--airflow"* ]]; then
    echo "Starting Airflow standalone..."
    airflow standalone
else
    echo "Airflow flag not provided. Use --airflow to start Airflow."
    echo "Environment variables are loaded and virtual environment is activated."
fi
