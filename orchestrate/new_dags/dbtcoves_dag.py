from fivetran_provider.sensors.fivetran import FivetranSensor
from airflow.decorators import dag, task_group
import datetime
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.bash import BashOperator
from fivetran_provider.operators.fivetran import FivetranOperator


@dag(
    default_args={"start_date": datetime.datetime(2023, 1, 1, 0, 0)},
    description="Blue / Green run",
    schedule_interval="0 0 1 */12 *",
    tags=["version_2"],
    catchup=False,
)
def dbtcoves_dag():
    @task_group(
        group_id="extract_and_load_airbyte", tooltip="Airbyte Extract and Load tasks"
    )
    def extract_and_load_airbyte():
        country_populations_datacoves_snowflake = AirbyteTriggerSyncOperator(
            task_id="country_populations_datacoves_snowflake",
            connection_id="ac02ea96-58a1-4061-be67-78900bb5aaf6",
            airbyte_conn_id="airbyte_connection",
        )
        personal_loans_datacoves_snowflake = AirbyteTriggerSyncOperator(
            task_id="personal_loans_datacoves_snowflake",
            connection_id="902432a8-cbed-4602-870f-33617fda6859",
            airbyte_conn_id="airbyte_connection",
        )
        country_populations_datacoves_snowflake >> personal_loans_datacoves_snowflake

    tg_extract_and_load_airbyte = extract_and_load_airbyte()

    @task_group(
        group_id="extract_and_load_fivetran", tooltip="Fivetran Extract and Load tasks"
    )
    def extract_and_load_fivetran():
        datacoves_snowflake_google_analytics_4_trigger = FivetranOperator(
            task_id="datacoves_snowflake_google_analytics_4_trigger",
            connector_id="speak_menial",
            do_xcom_push="True",
            fivetran_conn_id="fivetran_connection",
        )
        datacoves_snowflake_google_analytics_4_sensor = FivetranSensor(
            task_id="datacoves_snowflake_google_analytics_4_sensor",
            fivetran_conn_id="fivetran_connection",
            connector_id="speak_menial",
            poke_interval="60",
        )
        (
            datacoves_snowflake_google_analytics_4_trigger
            >> datacoves_snowflake_google_analytics_4_sensor
        )

    tg_extract_and_load_fivetran = extract_and_load_fivetran()
    transform = BashOperator(
        task_id="transform",
        bash_command="$DATACOVES__REPO_PATH/automate/blue_green_run.py -s 'tag:daily_run_airbyte+ tag:daily_run_fivetran+ -t prd'",
    )
    transform.set_upstream([tg_extract_and_load_airbyte, tg_extract_and_load_fivetran])
    marketing_automation = BashOperator(
        task_id="marketing_automation",
        bash_command="echo 'send data to marketing tool'",
    )
    marketing_automation.set_upstream([transform])
    update_catalog = BashOperator(
        task_id="update_catalog", bash_command="echo 'refresh data catalog'"
    )
    update_catalog.set_upstream([transform])
    tg_extract_and_load_airbyte


dag = dbtcoves_dag()
