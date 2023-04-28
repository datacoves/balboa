from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s

IMAGE_REPO = "datacoves/airflow-pandas"   # Replace <IMAGE REPO> by your docker image repo path
IMAGE_TAG = "latest"   # Replace <IMAGE TAG> by your docker image repo tag, or use "latest"

CONFIG = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base", image=f"{IMAGE_REPO}:{IMAGE_TAG}"
                )
            ]
        )
    ),
}

default_args = {
    "owner": "airflow",
    "email": "bruno@datacoves.com",
    "email_on_failure": True,
    "description": "Sample python custom image DAG",
}

with DAG(
    dag_id="custom_image_sample_dag",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["version_1"],
    description="Sample python custom image",
    schedule_interval="0 0 1 */12 *",
) as dag:

    my_task = BashOperator(
        task_id="custom_image_sample_task",
        executor_config=CONFIG,
        bash_command="echo SUCCESS!",
    )

    my_task
