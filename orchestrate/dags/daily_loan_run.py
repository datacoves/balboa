import datetime

from airflow.decorators import dag, task_group
from airflow.providers.airbyte.operators.airbyte import \
    AirbyteTriggerSyncOperator
from fivetran_provider_async.operators import FivetranOperator
from fivetran_provider_async.sensors import FivetranSensor
from operators.datacoves.bash import DatacovesBashOperator
from operators.datacoves.dbt import DatacovesDbtOperator

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
        country_populations_datacoves_snowflake = AirbyteTriggerSyncOperator(
            task_id="country_populations_datacoves_snowflake",
            connection_id="ac02ea96-58a1-4061-be67-78900bb5aaf6",
            airbyte_conn_id="airbyte_connection",
        )

    tg_extract_and_load_airbyte = extract_and_load_airbyte()

    @task_group(
        group_id="extract_and_load_fivetran", tooltip="Fivetran Extract and Load"
    )
    def extract_and_load_fivetran():
        datacoves_snowflake_google_analytics_4_trigger = FivetranOperator(
            task_id="datacoves_snowflake_google_analytics_4_trigger",
            fivetran_conn_id="fivetran_connection",
            connector_id="speak_menial",
            wait_for_completion=False,
        )
        datacoves_snowflake_google_analytics_4_sensor = FivetranSensor(
            task_id="datacoves_snowflake_google_analytics_4_sensor",
            fivetran_conn_id="fivetran_connection",
            connector_id="speak_menial",
            poke_interval=60,
        )
        (
            datacoves_snowflake_google_analytics_4_trigger
            >> datacoves_snowflake_google_analytics_4_sensor
        )

    tg_extract_and_load_fivetran = extract_and_load_fivetran()

    @task_group(group_id="extract_and_load_dlt", tooltip="dlt Extract and Load")
    def extract_and_load_dlt():
        load_us_population = DatacovesBashOperator(
            task_id="load_loads_data",
            bash_command="""
                cd load/dlt \
                && ./loans_data.py
            """,
            env={
                "UV_CACHE_DIR": "/tmp/uv_cache",
                "EXTRACT__NEXT_ITEM_MODE":"fifo",
                "EXTRACT__MAX_PARALLEL_ITEMS":"1",
                "EXTRACT__WORKERS":"1",
                "NORMALIZE__WORKERS":"1",
                "LOAD__WORKERS":"1",
            },
            append_env=True,
        )

    tg_extract_and_load_dlt = extract_and_load_dlt()
    transform = DatacovesDbtOperator(
        task_id="transform",
        bash_command="dbt build -s 'tag:daily_run_airbyte+ tag:daily_run_fivetran+'",
    )
    transform.set_upstream(
        [
            tg_extract_and_load_airbyte,
            tg_extract_and_load_dlt,
            tg_extract_and_load_fivetran,
        ]
    )
    marketing_automation = DatacovesBashOperator(
        task_id="marketing_automation",
        bash_command="echo 'send data to marketing tool'",
    )
    marketing_automation.set_upstream([transform])
    update_catalog = DatacovesBashOperator(
        task_id="update_catalog", bash_command="echo 'refresh data catalog'"
    )
    update_catalog.set_upstream([transform])


dag = daily_loan_run()
