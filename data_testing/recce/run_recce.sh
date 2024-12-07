#!/bin/bash

# Set error handling
set -e

# Configuration
DBT_PROJECT_PATH=/config/workspace/transform
RECCE_CONFIG_PATH=/config/workspace/data_testing/recce/recce.yml

recce run --target-base-path $DBT_PROJECT_PATH/logs --target-path $DBT_PROJECT_PATH/target --project-dir $DBT_PROJECT_PATH --config $RECCE_CONFIG_PATH
