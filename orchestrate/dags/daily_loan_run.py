import datetime
from airflow.decorators import dag, task, task_group

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
        def airbyte_sync():
            from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
            AirbyteTriggerSyncOperator(
                task_id="country_populations_datacoves_snowflake",
                connection_id="ac02ea96-58a1-4061-be67-78900bb5aaf6",
                airbyte_conn_id="airbyte_connection",
            ).execute({})

        airbyte_sync()

    tg_extract_and_load_airbyte = extract_and_load_airbyte()

    @task_group(group_id="extract_and_load_fivetran", tooltip="Fivetran Extract and Load")
    def extract_and_load_fivetran():
        @task
        def fivetran_trigger():
            from fivetran_provider_async.operators import FivetranOperator
            FivetranOperator(
                task_id="datacoves_snowflake_google_analytics_4_trigger",
                fivetran_conn_id="fivetran_connection",
                connector_id="speak_menial",
                wait_for_completion=False,
            ).execute({})

        @task
        def fivetran_sensor():
            from fivetran_provider_async.sensors import FivetranSensor
            FivetranSensor(
                task_id="datacoves_snowflake_google_analytics_4_sensor",
                fivetran_conn_id="fivetran_connection",
                connector_id="speak_menial",
                poke_interval=60,
            ).poke({})

        trigger = fivetran_trigger()
        sensor = fivetran_sensor()
        trigger >> sensor

    tg_extract_and_load_fivetran = extract_and_load_fivetran()

    @task_group(group_id="extract_and_load_dlt", tooltip="DLT Extract and Load")
    def extract_and_load_dlt():
        @task.datacoves_bash
        def dlt_load():
            return "cd load/dlt && ./loans_data.py"

        dlt_load()

    tg_extract_and_load_dlt = extract_and_load_dlt()

    @task.datacoves_dbt(
        connection_id="main"
    )
    def transform():
        return "dbt build -s 'tag:daily_run_airbyte+ tag:daily_run_fivetran+ -t prd'"

    transform_task = transform()
    transform_task.set_upstream(
        [
            tg_extract_and_load_airbyte,
            tg_extract_and_load_dlt,
            tg_extract_and_load_fivetran,
        ]
    )

    @task.datacoves_bash
    def marketing_automation():
        return "echo 'send data to marketing tool'"

    @task.datacoves_bash
    def update_catalog():
        return "echo 'refresh data catalog'"

    marketing_task = marketing_automation()
    catalog_task = update_catalog()
    transform_task >> [marketing_task, catalog_task]


dag = daily_loan_run()
