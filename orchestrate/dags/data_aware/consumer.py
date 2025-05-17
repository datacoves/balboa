"""
## Airflow Data Aware Consumer
This DAG shows how to create a consumer DAG triggered by a dataset
"""

from airflow.decorators import dag, task

from orchestrate.utils import datacoves_utils
from orchestrate.utils.datasets import LAMBDA_UPDATED_DATASET, DAG_UPDATED_DATASET

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args = datacoves_utils.set_default_args(
        owner = "Noel Gomez",
        owner_email = "noel@example.com"
    ),

    description = "Sample Consumer DAG",
    schedule = (LAMBDA_UPDATED_DATASET | DAG_UPDATED_DATASET),
    tags = ["sample"],
)
def data_aware_consumer_dag():
    @task
    def consume_a_dataset():
        print("I'm the consumer")

    consume_a_dataset()

data_aware_consumer_dag()
