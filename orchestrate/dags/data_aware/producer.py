"""
## Airflow Data Aware Producer
This DAG shows how to create a DAG that updates a dataset
"""

from airflow.decorators import dag, task

from orchestrate.utils import datacoves_utils
from orchestrate.utils.datasets import DAG_UPDATED_DATASET

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args = datacoves_utils.set_default_args(
        owner = "Noel Gomez",
        owner_email = "noel@example.com"
    ),

    description = "Sample Consumer DAG",
    schedule = datacoves_utils.set_schedule("0 0 1 */12 *"),
    tags = ["sample"],
)
def data_aware_producer_dag():
    @task(outlets=[DAG_UPDATED_DATASET])
    def produce_a_dataset():
        print("I'm the producer")

    produce_a_dataset()

data_aware_producer_dag()
