import datetime
from airflow.decorators import dag, task, task_group

@dag(
    default_args={"start_date": datetime.datetime(2024, 1, 1)},
    description="Loan Run",
    schedule="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def daily_loan_run():

    @task_group(
        group_id="extract_and_load_fivetran",
        tooltip="Fivetran Extract and Load"
    )
    def extract_and_load_fivetran():

        @task
        def fivetran_trigger():
            from fivetran_provider_async.operators import FivetranOperator
            FivetranOperator(
                task_id="datacoves_snowflake_google_analytics_4_trigger",
                connector_id="speak_menial",
                do_xcom_push=True,
                fivetran_conn_id="fivetran_connection",
            ).execute({})

        @task
        def fivetran_sensor():
            from fivetran_provider_async.sensors import FivetranSensor
            FivetranSensor(
                task_id="datacoves_snowflake_google_analytics_4_sensor",
                connector_id="speak_menial",
                poke_interval=60,
                fivetran_conn_id="fivetran_connection",
            ).poke({})

        trigger = fivetran_trigger()
        sensor = fivetran_sensor()
        trigger >> sensor

    tg_extract_and_load_fivetran = extract_and_load_fivetran()

    @task.datacoves_dbt(connection_id="main")
    def transform():
        return "dbt build -s 'tag:daily_run_fivetran+'"

    transform_task = transform()
    transform_task.set_upstream([tg_extract_and_load_fivetran])

dag = daily_loan_run()
