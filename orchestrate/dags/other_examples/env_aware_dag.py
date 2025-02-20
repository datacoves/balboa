import datetime
import os
from airflow.decorators import dag, task
from orchestrate.utils.datacoves import get_default_args  # Import from utils.py

@dag(
    default_args=get_default_args(
        owner = "CustomOwner",
        email = "custom@example.com",
        retries = 5  # Override default retries),  # Called at DAG parsing time (scheduler logs)
    ),
    description="Sample DAG for dbt build",
    schedule="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def env_aware_dag():

    @task
    def log_runtime_env():
        """Logs the environment variable during DAG execution"""
        runtime_env_var = os.getenv("DATACOVES__AIRFLOW_TYPE", "")
        print(f"DATACOVES__AIRFLOW_TYPE at task execution time: {runtime_env_var}")

    @task.datacoves_dbt(connection_id="main")
    def build_dbt():
        return "dbt runn -s personal_loans"

    log_runtime_env()  # Execution-time logging
    build_dbt()

dag = env_aware_dag()
