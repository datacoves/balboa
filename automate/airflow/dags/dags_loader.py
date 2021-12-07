import os
import dagfactory
import glob
import subprocess
from datetime import datetime
from airflow import DAG
from pathlib import Path


def is_same_commit():
    current_commit = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True).stdout.strip("\n")
    commit_file = "/home/airflow/last-commit"
    if Path(commit_file).exists():
        with open(commit_file, "r") as f:
            lines = ''.join(f.readlines())
            if lines == current_commit:
                return True
    with open(commit_file, "w") as f:
        f.writelines(current_commit)
    return False

def main():
    if not is_same_commit():
        dags_folder = os.environ.get("DATACOVES__YAML_DAGS_FOLDER")
        yaml_config_files = glob.glob(f"{dags_folder}/*.yml") + glob.glob(f"{dags_folder}/*.yaml")


        for config_file in yaml_config_files:
            dag_factory = dagfactory.DagFactory(os.path.abspath(config_file))
            dag_factory.generate_dags(globals())

        first_dag = [x for x in dag_factory.config.keys() if x != 'default'][0]

        if first_dag in globals() and globals()[first_dag].tasks:
            # Evaluating one random task so Airflow updates dags definition
            globals()[first_dag].tasks[0]


main()