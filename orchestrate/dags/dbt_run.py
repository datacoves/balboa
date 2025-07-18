"""
## Sample DAG showing how to run dbt
This DAG shows how to run a dbt task
"""

from airflow.decorators import dag, task
import datetime
@dag(
    doc_md = __doc__,
    catchup = False,
    default_args={"start_date": datetime.datetime(2024, 1, 1, 0, 0), "retries": 3},
    schedule = None,
    description="Sample DAG demonstrating how to run dbt in airflow",
    tags=["transform_2"],
)
def dbt_dag():

    @task.datacoves_dbt(
        connection_id="main"
    )
    def run_dbt():
        return "dbt debug"

    run_dbt()

dbt_dag()