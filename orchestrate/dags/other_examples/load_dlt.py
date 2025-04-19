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

    @task.datacoves_bash
    def load_us_population():
        from orchestrate.utils import datacoves_utils

        env_vars = datacoves_utils.set_dlt_env_vars({"destinations": ["main_load_keypair"]})
        env_exports = datacoves_utils.generate_env_exports(env_vars)

        return f"{env_exports}; cd load/dlt && ./us_population.py"

    load_us_population()

load_with_dlt()
