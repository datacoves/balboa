from airflow.decorators import dag
from kubernetes.client import models as k8s
from operators.datacoves.bash import DatacovesBashOperator

default_args = {
    "owner": "airflow",
    "email": "some_user@exanple.com",
    "email_on_failure": True,
    "description": "Sample python dag",
}


@dag(
    default_args={"start_date": "2021-01"},
    description="sample_script",
    schedule_interval="0 0 1 */12 *",
    tags=["version_2"],
    catchup=False,
)
def sample_script():
    sample_script = DatacovesBashOperator(
        task_id="sample_script",
        bash_command=f"python orchestrate/python_scripts/sample_script.py",
    )


dag = sample_script()
