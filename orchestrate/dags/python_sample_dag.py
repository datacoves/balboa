# import os

# import pendulum

# from airflow import DAG
# from airflow.decorators import task
# from airflow.operators.bash import BashOperator

# from kubernetes.client import models as k8s

# default_args = {
#     'owner': 'airflow',
#     'email': 'gomezn@convexa.ai',
#     'email_on_failure': True
# }

# with DAG(
#     dag_id="test_pandas",
#     default_args=default_args,
#     start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
#     catchup=False,
#     tags=["sample_tag"],
# ) as dag:
#     executor_config_template = {
#         "pod_override": k8s.V1Pod(
#             spec=k8s.V1PodSpec(
#                 containers=[
#                     k8s.V1Container(
#                         name="base", image="datacoves/airflow-pandas:latest"
#                     )
#                 ]
#             )
#         ),
#     }

#     task_x = BashOperator(
#         task_id="bash_executor_config",
#         executor_config=executor_config_template,
#         bash_command="echo SUCCESS"
#     )

#     fail = BashOperator(
#         task_id='failing',
#         bash_command="dates"
#     )

#     task_x >> fail
#     # task_x



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
    'email_on_failure': True
}

CONFIG = {
    "pod_override": k8s.V1Pod(
        spec = k8s.V1PodSpec(
            containers = [
                k8s.V1Container(
                    name = "base", image = f"{IMAGE_REPO}:{IMAGE_TAG}"
                )
            ]
        )
    ),
}

with DAG(
    dag_id="python_DAG",
    default_args=default_args,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["version_1"],
) as dag:

    successful_task = BashOperator(
        task_id = "bash_executor_config",
        executor_config = CONFIG,
        bash_command = "echo SUCCESS"
    )

    failing_task = BashOperator(
        task_id = 'failing',
        bash_command = "dates"
    )

    successful_task >> failing_task
