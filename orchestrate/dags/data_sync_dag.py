from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from operators.datacoves.data_sync import (
    DatacovesDataSyncOperatorRedshift,
    DatacovesDataSyncOperatorSnowflake,
)


@dag(schedule=None, catchup=False, tags=["version_3"])
def data_sync_dag():
    @task.datacoves_airflow_db_sync(db_type="snowflake", service_connection_name="main")
    def data_sync_snowflake():
        pass

    # data_sync_snowflake = DatacovesDataSyncOperatorSnowflake(
    #     destination_schema="public",
    #     tables=["ab_permission", "ab_role", "ab_user"],
    #     connection_name="snowflake",
    # )
    data_sync_snowflake()


data_sync_dag()
