from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s

# Replace with your docker image repo path
IMAGE_REPO = "datacoves/airflow-pandas"

# Replace with your docker image repo tag, or use "latest"
IMAGE_TAG = "latest"

default_args = {
    'owner': 'airflow',
    'email': 'gomezn@datacoves.com',
    'email_on_failure': True,
    'description': "Sample python dag"
}

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

with DAG(
    dag_id = "python_sample_dag",
    default_args = default_args,
    start_date = datetime(2023, 1, 1),
    catchup = False,
    tags = ["version_5"],
    description = "Sample python dag dbt run",
    schedule_interval = "0 0 1 */12 *"
) as dag:

    successful_task = BashOperator(
        task_id = "successful_task",
        executor_config = CONFIG,
        # bash_command = "echo SUCCESS"
        bash_command="source /opt/datacoves/virtualenvs/main/bin/activate && dbt-coves dbt -- build -s tag:daily_run"
    )

    failing_task = BashOperator(
        task_id = 'failing_task',
        bash_command = "some_non_existant_command"
    )

    successful_task >> failing_task
