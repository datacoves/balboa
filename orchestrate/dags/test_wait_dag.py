import datetime

from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator


@dag(
    default_args={"start_date": "2021-01"},
    description="Wait Run",
    schedule_interval="0 0 1 */12 *",
    tags=["version_3"],
    catchup=False,
)
def wait_dag():
    wait_for_10_min = DatacovesBashOperator(
        task_id="wait_for_10_min",
        bash_command=" wait 10m && echo 'ended'",
    )


dag = wait_dag()
