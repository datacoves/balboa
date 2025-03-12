import datetime

from airflow.decorators import dag, task
from orchestrate.include.datasets import LAMBDA_UPDATED_DATASET, DAG_UPDATED_DATASET


@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "retries": 1
    },
    description="Sample Producer DAG",
    schedule=(LAMBDA_UPDATED_DATASET | DAG_UPDATED_DATASET),
    tags=["transform"],
    catchup=False,
)
def data_aware_consumer_dag():
    @task
    def run_consumer():
        print("I'm the consumer")

    run_consumer()


dag = data_aware_consumer_dag()
