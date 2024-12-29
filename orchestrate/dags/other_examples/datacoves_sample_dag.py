"""## Datacoves Operators Sample DAG
This DAG is a sample using the Datacoves Airflow Operators"""

from airflow.decorators import dag, task_group
from airflow.models import Variable
from operators.datacoves.bash import DatacovesBashOperator
from operators.datacoves.dbt import DatacovesDbtOperator
from pendulum import datetime

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
    schedule = "0 0 1 */12 *",
)
def datacoves_sample_dag():
    @task_group(group_id="dbt_and_python", tooltip="This is a group that runs dbt then a custom python file")
    def dbt_and_python():
        dbt_task = DatacovesDbtOperator(
            task_id = "run_dbt_task",
            bash_command = "dbt debug",
        )

        var1= Variable.get("my_var1")
        var2= Variable.get("my_var2")
        var3= Variable.get("my_var3")

        # This is calling an external Python file after activating the venv
        # use this instead of the Python Operator as it will activate the pre-configured
        # Datacoves venv
        python_task = DatacovesBashOperator(
            task_id = "run_python_script",
            # Virtual Environment is automatically activated
            # activate_venv=True,
            bash_command = "python orchestrate/python_scripts/sample_script.py",
            env = {'VAR1': var1,
                   'VAR2': var2,
                   'VAR3': var3,
            }
        )

        # Define task dependencies
        python_task.set_upstream([dbt_task])

    dbt_and_python()

# Invoke Dag
dag = datacoves_sample_dag()
