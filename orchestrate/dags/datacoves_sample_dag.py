from pendulum import datetime
from airflow import DAG
from airflow.decorators import dag, task
from operators.datacoves.bash import DatacovesBashOperator

DATACOVES_VIRTIAL_ENV = '/opt/datacoves/virtualenvs/main/bin/activate'

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
def datacoves_sample_dag():

    # Calling dbt commands
    dbt_task = DatacovesBashOperator(
        task_id = "run_dbt_task",
        bash_command = "dbt-coves dbt -- debug"
    )

    # This is calling an external Python file after activating the venv
    # use this instead of the Python Operator
    python_task = DatacovesBashOperator(
        task_id = "run_python_script",
        bash_command = f" \
            python $DATACOVES__REPO_PATH//orchestrate/python_scripts/sample_script.py \
        "
    )

    # Define task dependencies
    python_task.set_upstream([dbt_task])

# Invoke Dag
dag = datacoves_sample_dag()
