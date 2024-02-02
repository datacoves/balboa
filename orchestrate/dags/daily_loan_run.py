import datetime

from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator


@dag(
    default_args={"start_date": "2021-01"},
    description="Loan Run",
    schedule_interval="0 0 1 */12 *",
    tags=["version_2"],
    catchup=False,
)
def daily_loan_run():
    check_for_dlt = DatacovesBashOperator(
        task_id="check_for_dlt", bash_command="pip show dlt"
    )


dag = daily_loan_run()
