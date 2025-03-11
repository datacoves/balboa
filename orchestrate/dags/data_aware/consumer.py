import datetime

from airflow.decorators import dag, task
from airflow.datasets import Dataset

LAMBDA_UPDATED_DATASET = Dataset("s3://my_bucket/my_folder/my_file.csv")
DAG_UPDATED_DATASET = Dataset("upstream_data")

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
