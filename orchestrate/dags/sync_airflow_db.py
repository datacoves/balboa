from airflow.decorators import dag
from operators.datacoves.data_sync import DatacovesDataSyncOperator


default_args = {
    "owner": "airflow",
    "email": "some_user@example.com",
    "email_on_failure": True,
    "description": "Sync Data Airflow Dag",
}


@dag(
    default_args={"start_date": "2021-01"},
    description="sync_data_script",
    schedule_interval="0 0 1 */12 *",
    tags=["version_2"],
    catchup=False,
)


def sync_airflow_db():
    sync_data_script = DatacovesDataSyncOperator(
        destination_type="snowflake",
        service_connection_name="load_airflow"
    )


dag = sync_airflow_db()
