import os
import dagfactory
import glob
from datetime import datetime
from airflow import DAG


dags_folder = os.environ.get("DATACOVES__YAML_DAGS_FOLDER")
yaml_config_files = glob.glob(f"{dags_folder}/*.yml") + glob.glob(f"{dags_folder}/*.yaml")


for config_file in yaml_config_files:
    dag_factory = dagfactory.DagFactory(os.path.abspath(config_file))
    dag_factory.generate_dags(globals())

first_dag = [x for x in dag_factory.config.keys() if x != 'default'][0]

if first_dag in globals() and globals()[first_dag].tasks:
    # Evaluating one random task so Airflow updates dags definition
    globals()[first_dag].tasks[0]
