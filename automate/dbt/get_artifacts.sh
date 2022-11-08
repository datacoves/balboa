#! /bin/bash

# Cause script to exit on error
set -e

cd $DBT_HOME

mkdir -p logs

dbt run-operation get_last_manifest | awk '/{/ { f = 1 } f'  | sed  "1s/.*/{/" > logs/manifest.json
LINES_IN_MANIFEST="$(wc -l < logs/manifest.json)"

if [ $LINES_IN_MANIFEST -eq 0 ]
then
    echo "Manifest for this version of dbt not found in Snowflake, contact the Snowflake administrator to load a updated manifest to snowflake."
    # This is used by github actions
    echo "::set-output name=manifest_found::false"

    # This is used by Jenkins
    # echo "false" >> temp_MANIFEST_FOUND.txt
else
    echo "Updated manifest from production"

    # This is used by github actions
    echo "::set-output name=manifest_found::true"

    # This is used by Jenkins
    # echo "true" >> temp_MANIFEST_FOUND.txt
fi
