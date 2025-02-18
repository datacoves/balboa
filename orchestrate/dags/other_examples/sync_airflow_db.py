"""## Datacoves Airflow db Sync Sample DAG
This DAG is a sample using the DatacovesDataSyncOperatorSnowflake Airflow Operator
to sync the Airflow Database to a target db
"""

from airflow.decorators import dag
from pendulum import datetime

@dag(
    doc_md = __doc__,
    default_args = {
        "start_date": datetime(2024, 1, 1),
        "retries": 3
    },
    description = "sync_data_script",
    schedule = "0 0 1 */12 *",
    tags = ["extract_and_load"],
    catchup = False,
)
def sync_airflow_db():
    # service connection name default is 'airflow_db_load'.
    # Destination type default is 'snowflake' (and the only one supported for now)
    from operators.datacoves.data_sync import DatacovesDataSyncOperatorSnowflake

    sync_data_script = DatacovesDataSyncOperatorSnowflake(
        service_connection_name="airflow_db_load",  # this can be omitted or changed to another service connection name.
    )

    sync_data_script

dag = sync_airflow_db()
