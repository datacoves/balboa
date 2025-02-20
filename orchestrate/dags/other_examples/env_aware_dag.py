import datetime
import os
from airflow.decorators import dag, task
from orchestrate.utils.datacoves import is_not_my_airflow # Import from utils.py

@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": is_not_my_airflow(),
        "retries": 0
    },
    description="Sample DAG for dbt build",
    schedule="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def env_aware_dag():

    @task.datacoves_dbt(connection_id="main")
    def build_dbt():
        return "dbt run -s personal_loans"
    build_dbt()

dag = env_aware_dag()
