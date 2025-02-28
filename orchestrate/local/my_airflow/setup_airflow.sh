#bin/bash

pip install uv
uv venv
uv pip install -r /config/workspace/orchestrate/local/my_airflow/requirements.txt
uv pip install -e /config/workspace/orchestrate/local/my_airflow/providers/datacoves
cp -r /config/workspace/orchestrate/local/my_airflow/plugins /config/airflow
cp -r /config/workspace/orchestrate/local/my_airflow/airflow.cfg /config/airflow/
