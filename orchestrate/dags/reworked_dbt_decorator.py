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

    @task.datacoves_dbt(
        connection_id="main",
        upload_results=True,
    )
    def dbt_ls():
        return "dbt ls"

    dbt_ls()
