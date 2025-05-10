#!/bin/bash

pip install uv
uv venv
uv pip install -r /config/workspace/orchestrate/local/dagster/requirements.txt
