"""
## dbt debug on BigQuery
Runs `dbt debug` with the profile the Datacoves dbt decorator builds from the
`bigquery_sa` Airflow connection (a Launchpad service connection).
"""

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args=datacoves_utils.set_default_args(
        owner = "Alejandro Morera",
        owner_email = "alejandro@datacoves.com"
    ),

    schedule = None,
    description="dbt debug against BigQuery through a pushed service connection",
    tags=["transform"],
)
def dbt_bigquery_dag():

    @task.datacoves_dbt(
        connection_id="bigquery_sa"
    )
    def run_dbt():
        return "dbt debug"

    run_dbt()

dbt_bigquery_dag()
