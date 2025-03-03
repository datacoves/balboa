import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG

with DAG(
    "dbt_decorator_rework",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt_decorator_rework", "version_5"],
) as dag:

    @task.datacoves_dbt(
        connection_id="main",
        download_files=False,
        upload_results=True,
        upload_tag="manual__2025-03-03T12:15:00.131343+00:00",  # TODO change to basic_dag id
        upload_manifest=False,
    )
    def upload_artifacts():
        return "dbt ls"

    @task.datacoves_dbt(
        connection_id="main",
        download_files=True,
        upload_results=False,
        upload_tag="manual__2025-03-03T12:15:00.131343+00:00",  # TODO change to basic_dag id
    )
    def download_artifacts(download_successful: bool = False):
        if download_successful:
            return "dbt ls"
        else:
            return "dbt debug"

    upload_artifacts() >> download_artifacts()
