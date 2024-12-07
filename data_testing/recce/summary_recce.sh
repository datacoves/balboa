#!/bin/bash

# Set error handling
set -e

# Configuration
DBT_PROJECT_PATH=/config/workspace/transform
RECCE_CONFIG_PATH=/config/workspace/data_testing/recce/recce.yml

recce summary --project-dir $DBT_PROJECT_PATH --config $RECCE_CONFIG_PATH recce_state.json >> summary.md
