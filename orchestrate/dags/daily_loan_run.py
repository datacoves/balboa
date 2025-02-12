import datetime

from airflow.decorators import dag, task_group
from fivetran_provider_async.operators import FivetranOperator
from fivetran_provider_async.sensors import FivetranSensor
from operators.datacoves.dbt import DatacovesDbtOperator

@dag(
    default_args={"start_date": "2024-01"},
    description="Loan Run",
    schedule="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def daily_loan_run():
    @task_group(
        group_id="extract_and_load_fivetran", tooltip="Fivetran Extract and Load"
    )
    def extract_and_load_fivetran():
        datacoves_snowflake_google_analytics_4_trigger = FivetranOperator(
            task_id="datacoves_snowflake_google_analytics_4_trigger",
            connector_id="speak_menial",
            do_xcom_push=True,
            fivetran_conn_id="fivetran_connection",
        )
        datacoves_snowflake_google_analytics_4_sensor = FivetranSensor(
            task_id="datacoves_snowflake_google_analytics_4_sensor",
            connector_id="speak_menial",
            poke_interval=60,
            fivetran_conn_id="fivetran_connection",
        )
        datacoves_snowflake_google_analytics_4_sensor.set_upstream(
            [datacoves_snowflake_google_analytics_4_trigger]
        )

    tg_extract_and_load_fivetran = extract_and_load_fivetran()
    transform = DatacovesDbtOperator(
        task_id="transform",
        bash_command="dbt build -s 'tag:daily_run_fivetran+'",
    )
    transform.set_upstream([tg_extract_and_load_fivetran])

dag = daily_loan_run()
