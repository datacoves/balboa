"""## Datacoves Bash Operator DAG
This DAG is a sample using the Datacoves Airflow Operators"""

from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator
from operators.datacoves.dbt import DatacovesDbtOperator
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
def datacoves_sample_dag():

    # Calling dbt commands
    dbt_task = DatacovesDbtOperator(
        task_id = "run_dbt_task",
        bash_command = "dbt debug",
    )

    # This is calling an external Python file after activating the venv
    # use this instead of the Python Operator
    python_task = DatacovesBashOperator(
        task_id = "run_python_script",
        # Virtual Environment is automatically activated
        # activate_venv=True,
        bash_command = "python orchestrate/python_scripts/sample_script.py"
    )

    # Define task dependencies
    python_task.set_upstream([dbt_task])


# Invoke Dag
dag = datacoves_sample_dag()
dag.doc_md = __doc__
