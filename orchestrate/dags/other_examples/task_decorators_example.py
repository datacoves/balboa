## Datacoves Bash Operator DAG
from airflow.decorators import dag, task
from pendulum import datetime

# Only here for reference, this is automatically activated by Datacoves Operator
DATACOVES_VIRTUAL_ENV = "/opt/datacoves/virtualenvs/main/bin/activate"

doc = """## Datacoves Bash Operator DAG
This DAG is a sample using the Datacoves Airflow Operators with Tasks"""

@dag(
    default_args={
        "start_date": datetime(2022, 10, 10),
        "owner": "Mayra Pena",
        "email": "mayran@example.com",
        "email_on_failure": True,
    },
    catchup=False,
    tags=["version_6"],
    description="Testing new decorators",
    schedule_interval="0 0 1 */12 *",
)
def task_decorators_example():

    @task.datacoves_dbt(
        connection_id="main"
    )
    def dbt_test() -> str:
        return "dbt debug"

    @task.datacoves_dbt(
        connection_id="main"
    )
    def dbt_run() -> str:
        return "dbt run"

    # Define task dependencies
    dbt_test() >> dbt_run()

# Invoke Dag
dag = task_decorators_example()
dag.doc_md = doc
