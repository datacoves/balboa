import os

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'email': 'sebastian@convexa.ai',
    'email_on_failure': True
}

with DAG(
    dag_id="test_pandas",
    default_args=default_args,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["sample_tag"],
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
        bash_command="dates"
    )

    task_x >> fail
