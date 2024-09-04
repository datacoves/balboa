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
    tags=["version_1"],
    description="Datacoves Sample dag",
    # This is a regular CRON schedule. Helpful resources
    # https://cron-ai.vercel.app/
    # https://crontab.guru/
    schedule_interval="0 0 1 */12 *",
)
def datacoves_dbtcoves_dag():

    # Calling dbt commands
    dbt_task = DatacovesDbtOperator(
        task_id="run_dbtcoves",
        bash_command="dbt-coves dbt -- ls",
    )
    dbt_task


# Invoke Dag
dag = datacoves_dbtcoves_dag()
dag.doc_md = __doc__
