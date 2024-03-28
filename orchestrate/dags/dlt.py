# import os
from airflow.decorators import dag
from operators.datacoves.data_sync import DatacovesDataSyncOperator

# DATACOVES_VIRTUAL_ENV = "/Users/martin/Development/datacoves/airflow_local_venv"

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
    # os.environ["DATACOVES__REPO_PATH"] = "/Users/martin/Development/datacoves/airflow_local/scripts"
    # os.environ["AIRFLOW__WEBSERVER__BASE_URL"] = "https://martin.datacoves.com"
    sync_data_script = DatacovesDataSyncOperator(
        destination_type="snowflake",
        service_connection_name="main"
    )


dag = sync_airflow_db()
