import datetime
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from datahub_airflow_plugin.entities import Dataset

@dag(
    default_args={"start_date": datetime.datetime(2024, 1, 1, 0, 0), "retries": 3},
    description="Loan Run",
    schedule="0 0 1 */12 *",
    tags=["extract_and_load", "transform", "marketing_automation", "update_catalog"],
    catchup=False,
)
def daily_loan_run():

    @task_group(group_id="extract_and_load_airbyte", tooltip="Airbyte Extract and Load")
    def extract_and_load_airbyte():
        @task
        def sync_airbyte():
            from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
            return AirbyteTriggerSyncOperator(
                task_id="country_populations_datacoves_snowflake",
                connection_id="b293aaea-6557-4506-8cfb-6b621ec4c6ef",
                airbyte_conn_id="airbyte_connection",
            ).execute({})

        sync_airbyte()

    tg_extract_and_load_airbyte = extract_and_load_airbyte()

    @task_group(group_id="extract_and_load_fivetran", tooltip="Fivetran Extract and Load")
    def extract_and_load_fivetran():

        @task
        def trigger_fivetran():
            from fivetran_provider_async.operators import FivetranOperator
            return FivetranOperator(
                task_id="datacoves_snowflake_google_analytics_4_trigger",
                fivetran_conn_id="fivetran_connection",
                connector_id="speak_menial",
                wait_for_completion=False,
            ).execute({})

        @task
        def sensor_fivetran():
            from fivetran_provider_async.sensors import FivetranSensor
            return FivetranSensor(
                task_id="datacoves_snowflake_google_analytics_4_sensor",
                fivetran_conn_id="fivetran_connection",
                connector_id="speak_menial",
                poke_interval=60,
            ).poke({})

        trigger = trigger_fivetran()
        sensor = sensor_fivetran()

        trigger >> sensor
        return sensor  # Return last task in the group

    tg_extract_and_load_fivetran = extract_and_load_fivetran()

    @task_group(group_id="extract_and_load_dlt", tooltip="dlt Extract and Load")
    def extract_and_load_dlt():
        @task.datacoves_bash(
            outlets=[Dataset("snowflake", "raw.loans_data.loans_data")],
            env={
                "UV_CACHE_DIR": "/tmp/uv_cache",
                "EXTRACT__NEXT_ITEM_MODE": "fifo",
                "EXTRACT__MAX_PARALLEL_ITEMS": "1",
                "EXTRACT__WORKERS": "1",
                "NORMALIZE__WORKERS": "1",
                "LOAD__WORKERS": "1",
            },
            append_env=True
        )
        def load_loans_data():
            return "cd load/dlt && ./loans_data.py"
        load_loans_data()

    tg_extract_and_load_dlt = extract_and_load_dlt()

    @task.datacoves_dbt(
        connection_id="main",
        inlets=[
            Dataset("snowflake", "raw.loans_data.loans_data"),
            Dataset("snowflake", "raw.google_analytics_4.engagement_events_report")
        ]
    )
    def transform():
        return "dbt build -s 'tag:daily_run_airbyte+ tag:daily_run_fivetran+ -t prd'"

    @task.datacoves_bash
    def marketing_automation():
        return "echo 'send data to marketing tool'"

    @task.datacoves_bash
    def update_catalog():
        return "echo 'refresh data catalog'"


    transform_task = transform()
    marketing_automation_task = marketing_automation()
    update_catalog_task = update_catalog()

    [tg_extract_and_load_airbyte, tg_extract_and_load_dlt, tg_extract_and_load_fivetran] >> transform_task
    transform_task >> [marketing_automation_task, update_catalog_task]

# Invoke DAG
dag = daily_loan_run()
