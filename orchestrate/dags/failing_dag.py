"""## Datacoves Bash Operator DAG
This DAG is a sample using the Datacoves Airflow Operators"""

from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator
from pendulum import datetime

# Only here for reference, this is automatically activated by Datacoves Operator
DATACOVES_VIRTUAL_ENV = "/opt/datacoves/virtualenvs/main/bin/activate"


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

    DatacovesBashOperator(task_id="run_fail_script", bash_command="sleep 1200")


# Invoke Dag
dag = datacoves_failing_dag()
dag.doc_md = __doc__
