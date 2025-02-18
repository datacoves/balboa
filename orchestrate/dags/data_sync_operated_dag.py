"""## Datacoves Airflow db Sync Sample DAG
This DAG is a sample using the DatacovesDataSyncOperatorSnowflake Airflow Operator
to sync the Airflow Database to a target db
"""

from airflow.decorators import dag, task
from operators.datacoves.data_sync import DatacovesDataSyncOperatorSnowflake
from pendulum import datetime


@dag(
    doc_md=__doc__,
    default_args={"start_date": datetime(2024, 1, 1), "retries": 3},
    description="sync_data_script",
    schedule="0 0 1 */12 *",
    tags=["extract_and_load", "version_1"],
    catchup=False,
)
def data_sync_operator():
    # service connection name default is 'airflow_db_load'.
    # Destination type default is 'snowflake' (and the only one supported for now)
    sync_data_script = DatacovesDataSyncOperatorSnowflake(
        service_connection_name="main",  # this can be omitted or changed to another service connection name.
    )

    sync_data_script


dag = data_sync_operator()
