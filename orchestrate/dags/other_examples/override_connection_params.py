import datetime
from airflow.decorators import dag, task
import os

AIRFLOW_TARGET = "prod" if os.environ.get("DATACOVES__AIRFLOW_TYPE",'') == "team_airflow" else "local_dev"

@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
        "retries": 3,
    },
    description="Sample DAG showing how to override airflow connection info",
    schedule="0 0 1 */12 *",
    tags=["transform"],
    catchup=False,
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

# Invoke DAG
override_connection_params()
