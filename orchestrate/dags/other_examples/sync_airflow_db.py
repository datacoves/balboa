"""## Datacoves Airflow db Sync Sample DAG
This DAG is a sample using the DatacovesDataSyncOperatorSnowflake Airflow Operator
to sync the Airflow Database to a target db
"""


from airflow.decorators import dag
from operators.datacoves.data_sync import DatacovesDataSyncOperatorSnowflake


@dag(
    default_args={"start_date": "2021-01"},
    description="sync_data_script",
    schedule_interval="0 0 1 */12 *",
    tags=["version_3"],
    catchup=False,
)
def sync_airflow_db():
    # service connection name default is 'airflow_db_load'.
    # Destination type default is 'snowflake' (and the only one supported for now)
    sync_data_script = DatacovesDataSyncOperatorSnowflake(
        service_connection_name="airflow_db_load",  # this can be omitted or changed to another service connection name.
    )


dag = sync_airflow_db()
dag.doc_md = __doc__
