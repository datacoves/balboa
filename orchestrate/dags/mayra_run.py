import datetime

from airflow.decorators import dag, task_group
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from operators.datacoves.bash import DatacovesBashOperator


@dag(
    default_args={"start_date": "2021-01"},
    description="Loan Run",
    schedule_interval="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def mayra_run():
    @task_group(group_id="extract_and_load_airbyte", tooltip="Airbyte Extract and Load")
    def extract_and_load_airbyte():
        personal_loans_datacoves_snowflake = AirbyteTriggerSyncOperator(
            task_id="personal_loans_datacoves_snowflake",
            connection_id="902432a8-cbed-4602-870f-33617fda6859",
            airbyte_conn_id="airbyte_connection",
        )
        country_populations_datacoves_snowflake = AirbyteTriggerSyncOperator(
            task_id="country_populations_datacoves_snowflake",
            connection_id="ac02ea96-58a1-4061-be67-78900bb5aaf6",
            airbyte_conn_id="airbyte_connection",
        )

    tg_extract_and_load_airbyte = extract_and_load_airbyte()
    extract_and_load_dlt = DatacovesBashOperator(
        task_id="extract_and_load_dlt",
        bash_command=" echo =========== && echo 'this is temporary until DatacovesBashOperator is updated' && dbt-coves dbt -- ls -s something echo =====rm_project_dir====== && project_dir=$(cat /tmp/dbt_coves_dbt_clone_path.txt) && rm -rf $project_dir echo =====CP_DATACOVES__REPO_PATH====== && cp -rpf $DATACOVES__REPO_PATH/ $project_dir && echo ====cd_project_dir_dlt======= && cd $project_dir/load/dlt && echo =====RUN_DLT====== && python csv_to_snowflake/load_csv_data.py && echo ===========",
    )
    transform = DatacovesBashOperator(
        task_id="transform",
        bash_command="dbt-coves dbt -- build -s 'tag:daily_run_airbyte+ -t prd'",
    )
    transform.set_upstream([tg_extract_and_load_airbyte, extract_and_load_dlt])
    marketing_automation = DatacovesBashOperator(
        task_id="marketing_automation",
        bash_command="echo 'send data to marketing tool'",
    )
    marketing_automation.set_upstream([transform])
    update_catalog = DatacovesBashOperator(
        task_id="update_catalog", bash_command="echo 'refresh data catalog'"
    )
    update_catalog.set_upstream([transform])


dag = mayra_run()
