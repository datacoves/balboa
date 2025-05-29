"""
## Retry dbt Example
This DAG how to retry a DAG that fails during a run
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
    description="Sample DAG demonstrating how to run the dbt models that fail",
    tags=["transform"],
)
def retry_dbt_failures():

    @task.datacoves_dbt(
        connection_id="main",
        dbt_api_enabled=True,
        upload_static_artifacts=True,
    )
    def dbt_compile():
        return "dbt compile"
        # This should generate manifest and other static assets
        # This should be done in the CI/CD after merging to main
        # because static dbt assets dont change

    @task.datacoves_dbt(
        connection_id="main",
        dbt_api_enabled=True,
        download_run_results=True,
        download_sources_json=True,
    )
    def dbt_build(expected_files: list = []):
        if expected_files:
            return "dbt build -s result:error+ --state logs"
        else:
            return "dbt build -s stg_personal_loans+"


    dbt_compile() >> dbt_build(expected_files=["run_results.json", "sources.json"])

retry_dbt_failures()
