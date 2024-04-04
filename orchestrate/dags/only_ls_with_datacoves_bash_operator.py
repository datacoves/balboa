from airflow.decorators import dag
from kubernetes.client import models as k8s
from operators.datacoves.bash import DatacovesBashOperator

default_args = {
    "owner": "airflow",
    "email": "some_user@exanple.com",
    "email_on_failure": True,
    "description": "Martin's dag",
}


@dag(
    default_args={"start_date": "2021-01"},
    description="Martin's dag",
    schedule_interval="0 0 1 */12 *",
    tags=["version_3"],
    catchup=False,
)
def martins_dag():
    first_task = DatacovesBashOperator(
        task_id="test_grid_first_Task",
        bash_command=f"echo 'First Task'",
    )
    second_task = DatacovesBashOperator(
        task_id="test_grid_second_Task",
        bash_command=f"echo 'Second Task'",
    )
    first_task >> second_task


dag = martins_dag()
