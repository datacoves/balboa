import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s

SUCCESSFUL_TASK_CONFIG = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="successful_task",
                    image="datacoves/airflow-pandas:latest",
                )
            ]
        )
    ),
}


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "airflow",
        "email": "gomezn@datacoves.com",
        "email_on_failure": True,
    },
    description="Sample yaml dag dbt run",
    schedule_interval="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def yaml_sample_dag():
    successful_task = BashOperator(
        task_id="successful_task",
        bash_command="pip show pandas",
        executor_config=SUCCESSFUL_TASK_CONFIG,
    )
    failing_task = BashOperator(
        task_id="failing_task", bash_command="some_non_existant_command"
    )
    failing_task.set_upstream([successful_task])
    successful_task


dag = yaml_sample_dag()
