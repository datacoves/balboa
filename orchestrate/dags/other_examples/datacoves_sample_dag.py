"""## Datacoves Operators Sample DAG
This DAG is a sample using the Datacoves Airflow Operators"""

from airflow.decorators import dag, task_group
from operators.datacoves.bash import DatacovesBashOperator
from operators.datacoves.dbt import DatacovesDbtOperator
from pendulum import datetime

# Only here for reference, this is automatically activated by Datacoves Operator
DATACOVES_VIRTUAL_ENV = "/opt/datacoves/virtualenvs/main/bin/activate"

@dag(
    doc_md = __doc__,
    default_args = {
        "start_date": datetime(2022, 10, 10),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
        "retries": 3,
    },
    catchup = False,
    tags = ["python_script"],
    description = "Datacoves Sample dag",
    # This is a regular CRON schedule. Helpful resources
    # https://cron-ai.vercel.app/
    # https://crontab.guru/
    schedule_interval="0 0 1 */12 *",
)
def datacoves_sample_dag():
    @task_group(group_id="extract_and_load_dlt", tooltip="dlt Extract and Load")
    def run_it():
        dbt_task = DatacovesDbtOperator(
            task_id = "run_dbt_task",
            bash_command = "dbt debug",
        )

    run_it()

    # var1= "YEEHAW"
    # # Calling dbt commands
    # dbt_task = DatacovesDbtOperator(
    #     task_id = "run_dbt_task",
    #     bash_command = "dbt debug",
    # )

    # # This is calling an external Python file after activating the venv
    # # use this instead of the Python Operator
    # python_task = DatacovesBashOperator(
    #     task_id = "run_python_script",
    #     # Virtual Environment is automatically activated
    #     # activate_venv=True,
    #     bash_command = "python orchestrate/python_scripts/sample_script.py",
    #     env={'VAR1': var1}
    # )

    # # Define task dependencies
    # python_task.set_upstream([dbt_task])

# Invoke Dag
dag = datacoves_sample_dag()
