#!/bin/bash

# Set error handling
set -e

# Configuration
VENV_PATH="/config/workspace/orchestrate/local/dagster/.venv"
DAGSTER_HOME="/tmp/dagster"
PROJECT_DIR="balboa"
PORT=8501

export DBT_PROFILES_DIR=/config/.dbt

# Check if virtual environment exists
if [ ! -d "$VENV_PATH" ]; then
    echo "Error: Virtual environment not found at $VENV_PATH"
    exit 1
fi

# Create Dagster home directory
mkdir -p "$DAGSTER_HOME"
export DAGSTER_HOME

# Activate virtual environment
source "$VENV_PATH/bin/activate"

# Check if activation was successful
if [ -z "$VIRTUAL_ENV" ]; then
    echo "Error: Failed to activate virtual environment"
    exit 1
fi

# Change to project directory
if [ ! -d "$PROJECT_DIR" ]; then
    echo "Error: Project directory '$PROJECT_DIR' not found"
    exit 1
fi
cd "$PROJECT_DIR"

# Get IP address and start Dagster
IP_ADDRESS=$(hostname -I | awk '{print $1}')
if [ -z "$IP_ADDRESS" ]; then
    echo "Error: Failed to get IP address"
    exit 1
fi

echo "Starting Dagster on http://$IP_ADDRESS:$PORT"
dagster dev -p $PORT --host "$IP_ADDRESS"
