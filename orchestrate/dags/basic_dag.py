import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG

with DAG(
    "basic",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["basic", "version_1"],
) as dag:

    @task.datacoves_dbt(
        connection_id="main",
        upload_results=False,
        download_files=False,
    )
    def dbt_ls(download_successful: bool = True):
        return "dbt ls"

    dbt_ls()
