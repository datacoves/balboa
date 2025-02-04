from airflow.decorators import dag
from operators.datacoves.data_sync import DatacovesDataSyncOperatorSnowflake


@dag(
    default_args={"start_date": "2021-01"},
    description="sync_some_tables_to_custom_schema",
    schedule_interval="0 0 1 */12 *",
    tags=["version_2"],
    catchup=False,
)
def snowflake_sync_airflow_tables():
    sync_some_tables_to_custom_schema = DatacovesDataSyncOperatorSnowflake(
        destination_schema="BRUNO_TABLES_DUMP",
        service_connection_name="main",
        tables=[
            "ab_permission",
            "ab_role",
            "ab_user",
            "dag",
            "dag_run",
            "dag_tag",
            "import_error",
            "job",
            "task_fail",
            "task_instance",
        ],
    )


dag = snowflake_sync_airflow_tables()
