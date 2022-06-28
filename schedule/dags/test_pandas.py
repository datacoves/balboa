import os

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.settings import AIRFLOW_HOME

from kubernetes.client import models as k8s

with DAG(
    dag_id="test_pandas",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example3"],
) as dag:
    executor_config_template = {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        image="datacoves/airflow-pandas:latest"
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
    # @task(executor_config=executor_config_template)
    # def task_with_template():
    #     print("test_pandas ran successfully")

    task_x
