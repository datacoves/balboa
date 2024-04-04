from airflow.decorators import dag
from kubernetes.client import models as k8s
from operators.datacoves.bash import DatacovesBashOperator

default_args = {
    "owner": "airflow",
    "email": "martin@datacoves.com",
    "email_on_failure": True,
    "description": "test grid",
}

@dag(
    default_args={"start_date": "2021-01"},
    description="Test Grid",
    schedule_interval="0 0 1 */12 *",
    tags=["version_2"],
    catchup=False,
)

def test_grid():
    first_task = DatacovesBashOperator(
        task_id="test_grid_first_Task",
        bash_command=f"echo 'First Task'",
    )

first_task
