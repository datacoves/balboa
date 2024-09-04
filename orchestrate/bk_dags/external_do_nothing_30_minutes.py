from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator
from pendulum import datetime

# Only here for reference, this is automatically activated by Datacoves Operator
DATACOVES_VIRTUAL_ENV = "/opt/datacoves/virtualenvs/main/bin/activate"


@dag(
    default_args={
        "start_date": datetime(2022, 10, 10),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    catchup=False,
    tags=["version_6"],
    description="Datacoves Sample dag",
    # This is a regular CRON schedule. Helpful resources
    # https://cron-ai.vercel.app/
    # https://crontab.guru/
    schedule_interval="0 0 1 */12 *",
)
def external_do_nothing():
    # This is calling an external Python file after activating the venv
    # use this instead of the Python Operator
    external_wait = DatacovesBashOperator(
        task_id="external_wait",
        # Virtual Environment is automatically activated
        # activate_venv=True,
        bash_command="python orchestrate/python_scripts/wait_30_minutes.py",
    )

    # Define task dependencies
    external_wait


# Invoke Dag
dag = external_do_nothing()
