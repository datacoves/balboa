#!/bin/bash

# Set error handling
set -e

# Configuration
PORT=8501
CHARTS_PROJECT_PATH="$(cd "$(dirname "$0")" && pwd)"

# Get IP address
IP_ADDRESS=$(hostname -I | awk '{print $1}')
if [ -z "$IP_ADDRESS" ]; then
    echo "Error: Failed to get IP address"
    exit 1
fi

cd "$CHARTS_PROJECT_PATH"
echo "Starting dbt Charts on http://$IP_ADDRESS:$PORT/vendor_activity/"
uvx --from dbt-charts --with dbt-snowflake dct serve --port "$PORT" --host "$IP_ADDRESS"
