import os
import sys
import time
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def print_pythonpath():
    pythonpath = os.environ.get("PYTHONPATH", "No PYTHONPATH set")
    print(f"PYTHONPATH: {pythonpath}")
    print(f"sys.path: {sys.path}")
    print("testing logs")
    for i in range(5):
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
    description="DAG for testing logs.",
    schedule="23 20 * * 1-5",
    tags=["version_1"],
    catchup=False,
)
def test_dag_logs():
    test_bash_operator = BashOperator(
        task_id='hello_world_task',
        bash_command='python -c "print(\'Hello, world!\')"'
    )

    print_env_python_python = PythonOperator(
        task_id="print_pythonpath_python",
        python_callable=print_pythonpath
    )

    print_env_python_python >> test_bash_operator

dag = test_dag_logs()
