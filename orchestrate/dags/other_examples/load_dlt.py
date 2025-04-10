"""
## dlthub load Example
This DAG shows how to load data with dlt
"""

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args=datacoves_utils.set_default_args(
        owner = "Noel Gomez",
        owner_email = "noel@example.com"
    ),

    schedule = datacoves_utils.set_schedule("0 0 1 */12 *"),
    description="Sample DAG demonstrating how to run dlt in airflow",
    tags=["extract_and_load"],
)
def load_with_dlt():

    # here we use a datacoves service connection called main_load
    @task.datacoves_bash(
        env = {**datacoves_utils.connection_to_env_vars("main_load"), **datacoves_utils.uv_env_vars()}
    )
    def load_us_population():
        return "cd load/dlt && ./us_population.py"

    load_us_population()

load_with_dlt()
