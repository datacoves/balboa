from pendulum import datetime
from airflow.decorators import dag, task
from operators.datacoves.bash import DatacovesBashOperator

@dag(
    default_args={
        "start_date": datetime(2022, 10, 10),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },

    catchup=False,
    tags = ["version_3"],
    description = "Datacoves Sample dag",

    # This is a regular CRON schedule. Helpful resources
    # https://cron-ai.vercel.app/
    # https://crontab.guru/
    schedule_interval = "0 0 1 */12 *"
)
def dbtcoves_sample_dag():

    # Calling dbt commands
    dbtcoves_task = DatacovesBashOperator(
        task_id = "dbtcoves_task",
        bash_command = "dbt-coves extract airbyte"
    )

# Invoke Dag
dag = dbtcoves_sample_dag()
