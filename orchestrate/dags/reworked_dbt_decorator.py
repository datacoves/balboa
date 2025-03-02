import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG

with DAG(
    "dbt_decorator_rework",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt_decorator_rework", "version_1"],
) as dag:

    # @task.datacoves_dbt(
    #     connection_id="main",
    #     upload_results=True,
    #     upload_tag="bruno-feb-28-2"
    # )
    # def dbt_ls(download_successful:bool=True):
    #     return "dbt ls"

    # dbt_ls()

    @task.datacoves_dbt(
        connection_id="main",
        download_files=True,
        upload_tag="bruno-feb-28"
    )
    def dbt_run(download_successful=False):
        if download_successful:
            return "dbt run"
        else:
            return "dbt debug"
    dbt_run()

    # dbt_ls() >> dbt_run()
