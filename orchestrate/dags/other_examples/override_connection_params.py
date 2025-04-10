"""
## Sample DAG showing how to override connection info
This DAG shows how to override connection info in a decorator
"""

import os

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils

AIRFLOW_TARGET = "prod" if os.environ.get("DATACOVES__AIRFLOW_TYPE",'') == "team_airflow" else "local_dev"

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args = datacoves_utils.set_default_args(
        owner = "Noel Gomez",
        owner_email = "noel@example.com"
    ),

    description="Sample DAG showing how to override airflow connection info",
    schedule = datacoves_utils.set_schedule("0 0 1 */12 *"),
    tags = ["transform"],
)
def override_connection_params():
    @task.datacoves_dbt(
        connection_id="main",
        target=AIRFLOW_TARGET,
        overrides = {"threads": 3}
        )
    def run_dbt():
        return "dbt debug"

    run_dbt()

override_connection_params()
