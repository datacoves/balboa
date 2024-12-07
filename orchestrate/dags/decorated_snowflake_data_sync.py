import datetime

from airflow.decorators import dag, task


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Bruno Antonellini",
        "email": "bruno@datacoves.com",
        "email_on_failure": False,
    },
    description="Sample DAG for dbt build",
    schedule_interval="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def decorated_snowflake_data_sync():
    @task.datacoves_airflow_db_sync(
        db_type="snowflake",
        destination_schema="test_schema_nov_19",
        service_connection_name="snowflake_main",
        # additional_tables=["additional_table_1", "additional_table_2"],
    )
    def data_sync_snowflake():
        pass

    data_sync_snowflake()


dag = decorated_snowflake_data_sync()
