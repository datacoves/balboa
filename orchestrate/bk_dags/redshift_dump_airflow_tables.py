from airflow.decorators import dag
from operators.datacoves.data_sync import DatacovesDataSyncOperatorRedshift


@dag(
    default_args={"start_date": "2021-01"},
    description="sync_some_tables",
    schedule_interval="0 0 1 */12 *",
    tags=["version_2"],
    catchup=False,
)
def redshift_sync_airflow_tables():
    sync_some_tables = DatacovesDataSyncOperatorRedshift(
        service_connection_name="redshift",
        destination_schema="BRUNO_TABLES_DUMP",
        tables=["task_fail", "task_instance"],
    )


dag = redshift_sync_airflow_tables()
