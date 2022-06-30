#! /bin/bash

# Cause script to exit on error
set -e

echo "DBT_MANIFEST_FOUND: $DBT_MANIFEST_FOUND"

if [ $DBT_MANIFEST_FOUND == "true" ]
then
    echo "dbt manifest found."
else
    echo "dbt manifest not found."
fi
