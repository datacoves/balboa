import pendulum
from airflow.decorators import task
from airflow.models.dag import DAG

with DAG(
    "dbt_decorator_rework",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt_decorator_rework", "version_16"],
) as dag:

    @task.datacoves_dbt(
        connection_id="main",
        upload_static_artifacts=True,
    )
    def dbt_source_freshness():
        return "dbt source freshness" # This should generate run_results and sources_json

    @task.datacoves_dbt(connection_id="main", upload_static_artifacts=False)
    def dbt_build():
        return "dbt seed && dbt build -s stg_personal_loans+" # Using 'dbt build' here to generate run results

    @task.datacoves_dbt(
        connection_id="main",
        download_run_results=True,
        download_sources_json=True,
    )
    def download_artifacts(expected_files: list = []):
        if expected_files:
            return "dbt build -s result:error+ --state logs"
        else:
            return "dbt build -s stg_personal_loans+"

        

    dbt_source_freshness() >> dbt_build() >> download_artifacts(expected_files=["run_results.json", "sources.json", "breaking_file.txt"])
