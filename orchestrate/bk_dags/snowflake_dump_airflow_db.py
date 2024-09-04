from airflow.decorators import dag
from operators.datacoves.data_sync import DatacovesDataSyncOperatorSnowflake


@dag(
    default_args={"start_date": "2021-01"},
    description="sync_data_script",
    schedule_interval="0 0 1 */12 *",
    tags=["version_2"],
    catchup=False,
)
def snowflake_sync_airflow_db():
    sync_entire_db = DatacovesDataSyncOperatorSnowflake(service_connection_name="main")


dag = snowflake_sync_airflow_db()
