import os

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email

from kubernetes.client import models as k8s

def send(**context):
    subject = "Test"
    body = f"""
        Test email
    """
    send_email("sebastian@convexa.ai", subject, body)

with DAG(
    dag_id="test_pandas",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["sample_tag"],
    on_failure_callback=send
) as dag:
    executor_config_template = {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base", image="datacoves/airflow-pandas:latest"
                    )
                ]
            )
        ),
    }

    task_x = BashOperator(
        task_id="bash_executor_config",
        executor_config=executor_config_template,
        bash_command="echo SUCCESS",
    )
    
    fail = BashOperator(
        task_id='failing',
        bash_command="false"
    )

    task_x >> fail
