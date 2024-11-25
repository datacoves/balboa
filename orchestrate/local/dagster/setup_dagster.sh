#!/bin/bash

virtualenv .venv
source /config/workspace/orchestrate/local/dagster/.venv/bin/activate
pip install -r /config/workspace/orchestrate/local/dagster/requirements.txt
