import datetime

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from operators.datacoves.bash import DatacovesBashOperator

MY_SOURCE = Dataset("upstream_data")

@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "retries": 1
    },
    description="Sample Producer DAG",
    schedule=[MY_SOURCE],
    tags=["transform"],
    catchup=False,
)
def data_aware_consumer_dag():
    @task
    def run_dbt():
        print("I'm the consumer")

    run_dbt()


dag = data_aware_consumer_dag()
