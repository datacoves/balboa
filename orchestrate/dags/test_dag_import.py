import os
import sys
import time
from datetime import datetime, timedelta
from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator
from airflow.operators.python import PythonOperator
from utils.test import test


def print_pythonpath():
    pythonpath = os.environ.get("PYTHONPATH", "No PYTHONPATH set")
    print(f"PYTHONPATH: {pythonpath}")
    print(f"sys.path: {sys.path}")
    for i in range(100):
        print(f"var {i}")
        time.sleep(1)

@dag(
    default_args={
        "owner": "Alejandro",
        "depends_on_past": False,
        "start_date": datetime.today() - timedelta(days=1),
        "email": "alejandro@datacoves.com",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    description="DAG for testing Python imports.",
    schedule="23 20 * * 1-5",
    tags=["version_2"],
    catchup=False,
)
def test_dag_import_utils():
    test()
    datacoves_dbt = DatacovesDbtOperator(
        task_id="test_dag_import",
        bash_command="dbt debug"
    )

    print_env_python_python = PythonOperator(
        task_id="print_pythonpath_python",
        python_callable=print_pythonpath
    )

    print_env_python_python >> datacoves_dbt

dag = test_dag_import_utils()
