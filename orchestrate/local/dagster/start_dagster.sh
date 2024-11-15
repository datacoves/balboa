#!/bin/bash

# Check if a filename was provided
if [ $# -eq 0 ]; then
    echo "Error: Please provide a Dagster Python file name"
    echo "Usage: $0 <python_file>"
    exit 1
fi

PYTHON_FILE="$1"

source ./.venv/bin/activate

mkdir -p /tmp/dagster
export DAGSTER_HOME=/tmp/dagster

hostname -I | awk '{print $1}' | xargs dagster dev -f "$PYTHON_FILE" -p 8501 --host
