"""## Datacoves Bash Operator DAG
This DAG is a sample using the Datacoves Airflow Operators"""

from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator
from pendulum import datetime


@dag(
    default_args={
        "start_date": datetime(2022, 10, 10),
        "owner": "Bruno Antonellini",
        "email": "bruno@datacoves.com",
        "email_on_failure": True,
    },
    catchup=False,
    tags=["version_4"],
    description="Datacoves blue-green run",
    # This is a regular CRON schedule. Helpful resources
    # https://cron-ai.vercel.app/
    # https://crontab.guru/
    schedule_interval="0 0 1 */12 *",
)
def datacoves_bluegreen_dag():

    # Calling dbt commands
    blue_green_run = DatacovesDbtOperator(
        task_id="blue_green_run",
        bash_command="dbt-coves blue-green --prod-db-env-var DATACOVES__MAIN__DATABASE --drop-staging-db-at-start --keep-staging-db-on-success --dbt-selector '-s personal_loans'",
    )
    blue_green_run


# Invoke Dag
dag = datacoves_bluegreen_dag()
