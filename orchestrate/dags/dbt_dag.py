"""
## Sample DAG showing how to run dbt
This DAG shows how to run a dbt task
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
    description="Sample DAG demonstrating how to run dbt in airflow",
    tags=["transform"],
)
def dbt_dag():

    @task.datacoves_dbt(
        connection_id="main_key_pair"
    )
    def run_dbt():
        return "dbt debug"

    run_dbt()

dbt_dag()
