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

    @task.bash()
    def clear_tmp():
        return "rm -rf /tmp/airflow_repo"

    @task.datacoves_dbt(
        connection_id="main_key_pair",
        dbt_api_enabled=True,
        download_run_results=True,
    )
    def dbt_build(expected_files: list = []):
        print(f"Expecting Files: =====> {expected_files}")
        if expected_files:
            return "dbt build -s 1+result:error+ --state logs"
        else:
            return "dbt build -s stg_personal_loans+"


    clear_tmp() >> dbt_build(expected_files=["run_results.json"])

retry_dbt_failures()
