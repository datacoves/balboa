#!/bin/bash

pip install uv
uv venv
source /config/workspace/orchestrate/local/dagster/.venv/bin/activate
uv pip install -r /config/workspace/orchestrate/local/dagster/requirements.txt
