import datetime
from airflow.decorators import dag, task
from orchestrate.utils.datacoves import connection_to_env_vars, uv_env_vars

@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
        "retries": 3
    },
    description="Sample DAG demonstrating how to run dlt in airflow",
    schedule="0 0 1 */12 *",
    tags=["extract_and_load"],
    catchup=False,
)
def run_dlt():

    # here we use a datacoves service connection called main_load
    @task.datacoves_bash(
        env = {**connection_to_env_vars("main_load"), **uv_env_vars()}
    )
    def load_us_population():
        return "cd load/dlt && ./loans_data.py"

    load_us_population()

# Invoke DAG
run_dlt()
