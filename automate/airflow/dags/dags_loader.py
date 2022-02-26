import os
import dagfactory
import glob
import pickle
import subprocess
from datetime import datetime, timezone
from airflow import DAG
from pathlib import Path


WORKING_DIR = Path("/home/airflow")
TIMEOUT = 300   # seconds


def is_running(running_flag):
    """Checks running flag file and return if process is still running"""
    if running_flag.exists():
        mtime = datetime.fromtimestamp(running_flag.stat().st_mtime, tz=timezone.utc)
        expired = (datetime.utcnow() - mtime).seconds > TIMEOUT
        if expired:
            running_flag.unlink()
        return not expired
    return False


def register_dags(all_dags):
    """Register all dags in globals for airflow to load them"""
    dagfactory.DagFactory.register_dags(all_dags, globals())
    dags_wo_default = [x for x in all_dags.keys() if x != 'default']
    if dags_wo_default:
        random_dag = dags_wo_default[0]
        if random_dag in globals() and globals()[random_dag].tasks:
            # Evaluating one random task so Airflow updates dags definition
            globals()[random_dag].tasks[0]


def main():
    dags_folder = os.environ.get("DATACOVES__YAML_DAGS_FOLDER")
    current_commit = subprocess.run(['git', 'rev-parse', 'HEAD'],
        capture_output=True,
        text=True,
        cwd=dags_folder).stdout.strip("\n")
    print(f"Generating dags for commit '{current_commit}'")
    
    running_flag = WORKING_DIR / "running"
    if is_running(running_flag):
        return
    else:
        running_flag.touch()
    
    try:
        current_pickle = WORKING_DIR / f"{current_commit}.pickle"
        if current_pickle.exists():
            # Load cached dags
            with open(current_pickle, "rb") as f:
                all_dags = pickle.load(f)
        else:
            # Recalculate dags
            yaml_config_files = glob.glob(f"{dags_folder}/*.yml") + glob.glob(f"{dags_folder}/*.yaml")

            all_dags = dict()
            for config_file in yaml_config_files:
                dag_factory = dagfactory.DagFactory(os.path.abspath(config_file))
                dags = dag_factory.build_dags()
                all_dags.update(dags)
            with open(current_pickle, "wb") as f:
                pickle.dump(all_dags, f)

        register_dags(all_dags)
    finally:
        running_flag.unlink()

main()
