#bin/bash

virtualenv /config/workspace/orchestrate/local/my_airflow/.venv
source /config/workspace/orchestrate/local/my_airflow/.venv/bin/activate
pip install -r /config/workspace/orchestrate/local/my_airflow/requirements.txt
pip install /config/workspace/orchestrate/local/my_airflow/providers/datacoves
cp -r /config/workspace/orchestrate/local/my_airflow/plugins /config/airflow
cp -r /config/workspace/orchestrate/local/my_airflow/airflow.cfg /config/airflow/
