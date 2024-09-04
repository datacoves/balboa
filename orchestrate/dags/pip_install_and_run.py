"""## Datacoves Bash Operator DAG
This DAG is a sample using the Datacoves Airflow Operators"""

from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator
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
def pip_install_and_run():

    # This is calling an external Python file after activating the venv
    # use this instead of the Python Operator
    pip_install_dbt_coves_blue_green = DatacovesBashOperator(
        task_id="pip_install_dbt_coves_blue_green",
        # Virtual Environment is automatically activated
        # activate_venv=True,
        bash_command="pip install -U git+https://github.com/datacoves/dbt-coves.git@DCV-2857-dbt-coves-changes-to-blue-green && dbt-coves blue-green -h",
    )

    # Define task dependencies
    pip_install_dbt_coves_blue_green


# Invoke Dag
dag = pip_install_and_run()
dag.doc_md = __doc__
