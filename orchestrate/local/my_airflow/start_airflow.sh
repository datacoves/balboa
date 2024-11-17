#!/bin/bash

# Activate the virtual environment
source ./.venv/bin/activate

# Load environment variables
export $(cat .env | xargs)

# Check if --airflow flag is present
if [[ "$*" == *"--airflow"* ]]; then
    echo "Starting Airflow standalone..."
    airflow standalone
else
    echo "Airflow flag not provided. Use --airflow to start Airflow."
    echo "Environment variables are loaded and virtual environment is activated."
fi
