#!/bin/bash

# Set error handling
set -e

# Configuration
PORT=8501

# Get IP address and start Dagster
IP_ADDRESS=$(hostname -I | awk '{print $1}')
if [ -z "$IP_ADDRESS" ]; then
    echo "Error: Failed to get IP address"
    exit 1
fi

uvx visivo serve -p $PORT
# -h "$IP_ADDRESS"
# echo "Starting Dagster on http://$IP_ADDRESS:$PORT"
# uv run --no-project dagster dev -p $PORT --host "$IP_ADDRESS"
