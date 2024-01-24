import datetime

from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator


@dag(
    default_args={"start_date": "2021-01"},
    description="Loan Run",
    schedule_interval="0 0 1 */12 *",
    tags=["version_33"],
    catchup=False,
)
def daily_loan_run():
    extract_and_load_dlt = DatacovesBashOperator(
        task_id="extract_and_load_dlt",
        bash_command=" echo =========== && echo 'this is temporary until DatacovesBashOperator is updated' && dbt-coves dbt -- ls -s somehting echo =========== && pwd && echo =========== && project_dir=$(cat /tmp/dbt_coves_dbt_clone_path.txt) && cd $project_dir && ls -la && echo ======VAR===== && echo $project_dir && echo ======REPO_PATH===== && echo $DATACOVES__REPO_PATH && cd $DATACOVES__REPO_PATH && ls -la && echo =========== && cp -rpf $DATACOVES__REPO_PATH $project_dir && echo =========== && cd $project_dir && echo =========== && ls -la && echo =========== && cd .. && echo =========== && ls -la && echo =========== && cd ../load/dlt/ && echo =========== && ls -la && echo =========== && python csv_to_snowflake/load_csv_data.py && echo ===========",
    )


dag = daily_loan_run()
