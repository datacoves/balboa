"""
## Sample DAG showing end-to-end ELT
This DAG shows how to load data with 3 tools, then run dbt, then other tasks
"""

from airflow.decorators import dag, task, task_group
from orchestrate.utils import datacoves_utils

from fivetran_provider_async.operators import FivetranOperator
from fivetran_provider_async.sensors import FivetranSensor
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args = datacoves_utils.set_default_args(
        owner = "Noel Gomez",
        owner_email = "noel@example.com"
    ),

    description = "Sample DAG to synchronize the Airflow database",
    schedule = datacoves_utils.set_schedule("0 0 1 */12 *"),
    tags=["extract_and_load", "transform", "marketing_automation", "update_catalog"],
)
def daily_loan_run():

    @task_group(
        group_id="extract_and_load_airbyte",
        tooltip="Airbyte Extract and Load"
    )
    def extract_and_load_airbyte():
        # Extact and load
        sync_airbyte = AirbyteTriggerSyncOperator(
            task_id="country_populations_datacoves_snowflake",
            connection_id="ac02ea96-58a1-4061-be67-78900bb5aaf6",
            airbyte_conn_id="airbyte_connection",
        )


    @task_group(
        group_id="extract_and_load_fivetran",
        tooltip="Fivetran Extract and Load"
    )
    def extract_and_load_fivetran():
        trigger_fivetran =  FivetranOperator(
            task_id="datacoves_snowflake_google_analytics_4_trigger",
            fivetran_conn_id="fivetran_connection",
            connector_id="speak_menial",
            wait_for_completion=False,
        )
        sensor_fivetran = FivetranSensor(
            task_id="datacoves_snowflake_google_analytics_4_sensor",
            fivetran_conn_id="fivetran_connection",
            connector_id="speak_menial",
            poke_interval=60,
        )
        trigger_fivetran >> sensor_fivetran


    @task_group(
        group_id="extract_and_load_dlt",
        tooltip="dlt Extract and Load"
    )
    def extract_and_load_dlt():
        @task.datacoves_bash
        def load_loans_data():
            from orchestrate.utils import datacoves_utils

            env_vars = datacoves_utils.set_dlt_env_vars({"destinations": ["main_load_keypair"]})
            env_exports = datacoves_utils.generate_env_exports(env_vars)

            return f"{env_exports}; cd load/dlt && ./loans_data.py"

        load_loans_data()


    # Transform Data
    @task.datacoves_dbt(
        connection_id="main_key_pair"
    )
    def transform():
        return "dbt build -s 'tag:daily_run_airbyte+ tag:daily_run_fivetran+'"


    # Post transformation tasks
    @task.datacoves_bash
    def marketing_automation():
        return "echo 'send data to marketing tool'"

    @task.datacoves_bash
    def update_catalog():
        return "echo 'refresh data catalog'"

    extract_and_load = [extract_and_load_airbyte(), extract_and_load_fivetran(), extract_and_load_dlt()]
    extract_and_load >> transform() >> [marketing_automation(), update_catalog()]

daily_loan_run()
