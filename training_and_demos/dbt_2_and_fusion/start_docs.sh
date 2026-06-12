#!/bin/bash

# Set error handling
set -e

# Configuration
PORT=8501

# Get IP address
IP_ADDRESS=$(hostname -I | awk '{print $1}')
if [ -z "$IP_ADDRESS" ]; then
    echo "Error: Failed to get IP address"
    exit 1
fi

echo "Starting dbt docs on http://$IP_ADDRESS:$PORT"
dbt compile --use-index --static-analysis strict --write-lineage -t prd
dbt docs serve --port $PORT --host "$IP_ADDRESS"
