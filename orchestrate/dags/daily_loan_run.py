import datetime

from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator


@dag(
    default_args={"start_date": "2021-01"},
    description="Loan Run",
    schedule_interval="0 0 1 */12 *",
    tags=["version_37"],
    catchup=False,
)
def daily_loan_run():
    extract_and_load_dlt = DatacovesBashOperator(
        task_id="extract_and_load_dlt",
        bash_command=" echo =========== && echo 'this is temporary until DatacovesBashOperator is updated' && dbt-coves dbt -- ls -s somehting echo =====rm_project_dir====== && project_dir=$(cat /tmp/dbt_coves_dbt_clone_path.txt) && rm -rf $project_dir echo =====CP_DATACOVES__REPO_PATH====== && cp -rpf $DATACOVES__REPO_PATH/ $project_dir && echo ====cd_project_dir_dlt======= && cd $project_dir/load/dlt && echo =====ls_project_dir====== && ls -la && echo =====RUN_DLT====== && python csv_to_snowflake/load_csv_data.py && echo ===========",
    )


dag = daily_loan_run()
