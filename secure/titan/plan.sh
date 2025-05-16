#!/bin/bash

if [ -f .env ]; then
    echo "File .env found."
else
    echo "File .env does not exist. Please create a .env file with the following variables:"
    echo ""
    echo "SNOWFLAKE_ACCOUNT="
    echo "SNOWFLAKE_USER="
    echo "SNOWFLAKE_PASSWORD="
    echo "SNOWFLAKE_ROLE="
    echo ""
    exit 1
fi

export $(cat .env | xargs)

titan plan --config resources/
