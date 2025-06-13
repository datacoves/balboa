"""
## Datacoves Blue-Green DAG
This DAG demonstrates how to do a blue-green run
"""

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args = datacoves_utils.set_default_args(
        owner = "Bruno",
        owner_email = "bruno@example.com"
    ),

    description="Datacoves blue-green run",
    schedule = datacoves_utils.set_schedule("@daily"),
    tags=["transform"],
)
def blue_green_dbt_run():

    @task.datacoves_dbt(connection_id="main_key_pair")
    def blue_green_run():
        return "dbt-coves blue-green --dbt-selector '-s personal_loans'"

    blue_green_run()

blue_green_dbt_run()
