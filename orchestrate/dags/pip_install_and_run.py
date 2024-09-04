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
    tags=["version_3"],
    description="Datacoves Sample dag",
    schedule_interval="0 0 1 */12 *",
)
def pip_install_and_run():
    tell_me_a_joke = DatacovesBashOperator(
        task_id="pip_install_dbt_coves_blue_green",
        bash_command="pip install pyjokes && pyjoke",
    )

    # Define task dependencies
    tell_me_a_joke


# Invoke Dag
dag = pip_install_and_run()
dag.doc_md = __doc__
