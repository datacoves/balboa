import datetime
from airflow.decorators import dag, task

@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
        "retries": 3,
    },
    description="Sample DAG for dbt build",
    schedule="0 0 1 */12 *",
    tags=["transform"],
    catchup=False,
)
def yaml_dbt_dag():

    @task.datacoves_dbt(connection_id="keypair_test")
    def run_dbt():
        return "dbt debug"

    run_dbt()

# Invoke DAG
dag = yaml_dbt_dag()
