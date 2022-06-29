#! /bin/bash

# Cause script to exit on error
set -e

cd $DBT_HOME

dbt run-operation get_last_manifest | awk '/{/ { f = 1 } f'  | sed  "1s/.*/{/" > logs/manifest.json
LINES_IN_MANIFEST="$(wc -l < logs/manifest.json)"

if [ $LINES_IN_MANIFEST -eq 0 ]
then
    echo "Manifest for this version of dbt not found in Snowflake, contact the Snowflake administrator to load a updated manifest to snowflake."
    export DBT_RETURN_CODE=1
else
    echo "Updated manifest from production"
    export DBT_RETURN_CODE=0
fi
