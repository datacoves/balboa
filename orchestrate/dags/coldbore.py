from airflow.decorators import dag
from kubernetes.client import models as k8s
from operators.datacoves.bash import DatacovesBashOperator

# Replace with your docker image repo path
IMAGE_REPO = "python"
# Replace with your docker image repo tag, or use "latest"
IMAGE_TAG = "3.8.18-slim-bullseye"

default_args = {
    "owner": "airflow",
    "email": "some_user@exanple.com",
    "email_on_failure": True,
    "description": "Sample python dag",
}

TRANSFORM_CONFIG = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[k8s.V1Container(name="base", image=f"{IMAGE_REPO}:{IMAGE_TAG}")]
        )
    ),
}


@dag(
    default_args={"start_date": "2021-01"},
    description="k8s_executor",
    schedule_interval="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def k8s_executor():
    k8s_executor = DatacovesBashOperator(
        task_id="k8s_executor",
        executor_config=TRANSFORM_CONFIG,
        bash_command=f"echo Hello",
    )


dag = k8s_executor()
