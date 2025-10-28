#! /bin/bash

# Cause script to exit on error
set -e

cd $DATACOVES__DBT_HOME

mkdir -p logs

dbt run-operation get_last_artifacts

# Check if manifest.json exists before trying to grep it
if [ -f "logs/manifest.json" ]; then
    LINES_IN_MANIFEST="$(grep -c '^' logs/manifest.json)"
else
    LINES_IN_MANIFEST=0
fi

if [ $LINES_IN_MANIFEST -eq 0 ]
then
    echo "Manifest not found in Snowflake stage, contact the Snowflake administrator to load a updated manifest.json to snowflake."
    # This is used by github actions
    if [ -n "$GITHUB_OUTPUT" ]; then
        echo "manifest_found=false" >> $GITHUB_OUTPUT
    fi
    echo $manifest_found
    # This is used by Jenkins
    # echo "false" > temp_MANIFEST_FOUND.txt
else
    echo "Updated manifest from production"

    # This is used by github actions
    if [ -n "$GITHUB_OUTPUT" ]; then
        echo "manifest_found=true" >> $GITHUB_OUTPUT
    fi

    # This is used by Jenkins
    # echo "true" > temp_MANIFEST_FOUND.txt
fi


# Check if catalog.json exists before trying to grep it
if [ -f "logs/catalog.json" ]; then
    LINES_IN_CATALOG="$(grep -c '^' logs/catalog.json)"
else
    LINES_IN_CATALOG=0
fi


if [ $LINES_IN_CATALOG -eq 0 ]
then
    echo "Catalog not found in Snowflake stage, contact the Snowflake administrator to load a updated catalog.json to snowflake."
    # This is used by github actions
    if [ -n "$GITHUB_OUTPUT" ]; then
        echo "catalog_found=false" >> $GITHUB_OUTPUT
    fi
else
    echo "Updated catalog from production"

    # This is used by github actions
    if [ -n "$GITHUB_OUTPUT" ]; then
        echo "catalog_found=true" >> $GITHUB_OUTPUT
    fi
fi
