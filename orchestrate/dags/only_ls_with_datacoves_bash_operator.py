from airflow.decorators import dag
from kubernetes.client import models as k8s
from operators.datacoves.bash import DatacovesBashOperator

default_args = {
    "owner": "airflow",
    "email": "some_user@exanple.com",
    "email_on_failure": True,
    "description": "Sample ls -alh dag",
}


@dag(
    default_args={"start_date": "2021-01"},
    description="ls -alh",
    schedule_interval="0 0 1 */12 *",
    tags=["version_2"],
    catchup=False,
)
def ls_alh():
    ls_alh = DatacovesBashOperator(
        task_id="ls_alh",
        bash_command=f"ls -alh",
    )


dag = ls_alh()
