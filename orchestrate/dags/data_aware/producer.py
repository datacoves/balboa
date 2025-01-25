import datetime

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from operators.datacoves.bash import DatacovesBashOperator

# A dataset can be anything, it will be a poiner in the Airflow db.
# If you need to access url like s3://my_bucket/my_file.txt then you can set
# it with the proper path for reuse.
MY_SOURCE = Dataset("upstream_data")

@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "retries": 3
    },
    description="Sample Producer DAG",
    schedule="0 0 1 */12 *",
    tags=["extract_and_load"],
    catchup=False,
)
def data_aware_producer_dag():
    @task(outlets=[MY_SOURCE])
    def extract_and_load_dlt():
        print("I'm the producer")

    extract_and_load_dlt()


dag = data_aware_producer_dag()
