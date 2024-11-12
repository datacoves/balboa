"""## Datacoves Blue-Green DAG"""

from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator
from pendulum import datetime


@dag(
    default_args={
        "start_date": datetime(2024, 1, 1),
        "owner": "Datacoves",
        "email": "bruno@example.com",
        "email_on_failure": True,
    },
    catchup = False,
    tags = ["version_1"],
    description = "Datacoves blue-green run",
    schedule = "@daily",
)
def datacoves_bluegreen_dag():
    blue_green_run = DatacovesDbtOperator(
        task_id="blue_green_run",
        bash_command="dbt-coves blue-green --dbt-selector '-s personal_loans'",
    )
    blue_green_run


# Invoke Dag
dag = datacoves_bluegreen_dag()
dag.doc_md = __doc__
