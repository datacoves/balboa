#bin/bash

virtualenv .venv
source ./.venv/bin/activate
pip install -r ./requirements.txt
pip install ./providers/datacoves
cp -r ./plugins /config/airflow
cp -r ./airflow.cfg /config/airflow/
