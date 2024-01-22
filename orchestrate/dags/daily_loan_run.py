import datetime

from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator


@dag(
    default_args={"start_date": "2021-01"},
    description="Loan Run",
    schedule_interval="0 0 1 */12 *",
    tags=["version_14"],
    catchup=False,
)
def daily_loan_run():
    extract_and_load_dlt = DatacovesBashOperator(
        task_id="extract_and_load_dlt",
        bash_command=" echo =========== && echo 'this is temporary until DatacovesBashOperator is updated' && dbt-coves dbt -- ls -s somehting echo =========== && pwd && cd '$(cat /tmp/dbt_coves_dbt_clone_path.txt)' && cd ../load/dlt/ && echo =========== && python csv_to_snowflake/load_csv_data.py && echo ===========",
    )


dag = daily_loan_run()
