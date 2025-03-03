import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG

with DAG(
    "basic_dag",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["basic_dag", "version_2"],
) as dag:

    @task.datacoves_dbt(
        connection_id="main",
        download_files=False,
        upload_results=False,
    )
    def dbt_ls(download_successful: bool = False):
        return "dbt ls"

    dbt_ls()
