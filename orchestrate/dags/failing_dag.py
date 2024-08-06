"""## Datacoves Bash Operator DAG
This DAG is a sample using the Airflow Operators"""

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime

@dag(
    default_args={
        "start_date": datetime(2022, 10, 10),
        "owner": "Sebsatian Sassi",
        "email": "sebastian@datacoves.com",
        "email_on_failure": True,
    },
    catchup=False,
    tags=["version_1"],
    description="Datacoves Failing dag",
    schedule_interval="0 0 1 */12 *",
)
def datacoves_failing_dag():

    BashOperator(task_id="run_script", bash_command="echo 'success'")


# Invoke Dag
dag = datacoves_failing_dag()
dag.doc_md = __doc__
