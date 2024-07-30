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
    description="Datacoves Sample dag",
    schedule_interval="0 0 1 */12 *",
)
def datacoves_sample_dag():

    DatacovesBashOperator(task_id="run_python_script", bash_command="fail")


# Invoke Dag
dag = datacoves_sample_dag()
dag.doc_md = __doc__
