"""## Datacoves Blue-Green DAG"""

from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator
from pendulum import datetime


@dag(
    default_args={
        "start_date": datetime(2024, 8, 26),
        "owner": "Datacoves",
        "email": "bruno@datacoves.com",
        "email_on_failure": True,
    },
    catchup=False,
    tags=["version_1"],
    description="Datacoves blue-green run",
    schedule_interval="0 0 1 */12 *",
)
def datacoves_bluegreen_dag():
    blue_green_run = DatacovesDbtOperator(
        task_id="blue_green_run",
        bash_command="dbt-coves blue-green --service-connection-name MAIN --drop-staging-db-at-start --keep-staging-db-on-success --dbt-selector '-s personal_loans'",
    )
    blue_green_run


# Invoke Dag
dag = datacoves_bluegreen_dag()
