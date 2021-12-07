#! /bin/bash

# Cause script to exit on error
set -e

cd $DBT_HOME
echo $DBT_DATABASE
dbt run-operation get_last_manifest --args '{"artifacts_location": "balboa.dbt_artifacts.artifacts"}' | awk '/{/ { f = 1 } f' > logs/manifest.json
echo "Updated manifest from production"