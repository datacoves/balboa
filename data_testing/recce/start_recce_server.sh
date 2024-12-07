#!/bin/bash

# Set error handling
set -e

# Configuration
PORT=8501
DBT_PROJECT_PATH=/config/workspace/transform
RECCE_CONFIG_PATH=/config/workspace/data_testing/recce/recce.yml

# Get IP address
IP_ADDRESS=$(hostname -I | awk '{print $1}')
if [ -z "$IP_ADDRESS" ]; then
    echo "Error: Failed to get IP address"
    exit 1
fi

echo "Starting Recce on http://$IP_ADDRESS:$PORT"
recce server --port $PORT --host "$IP_ADDRESS" --target-base-path $DBT_PROJECT_PATH/logs --target-path $DBT_PROJECT_PATH/target --project-dir $DBT_PROJECT_PATH --config $RECCE_CONFIG_PATH
