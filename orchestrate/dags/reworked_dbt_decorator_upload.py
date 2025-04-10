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
        # download_static_artifacts=False,
        upload_static_artifacts=True, #semantic_manifest, partial_parse
        # upload_tag="manual__2025-03-27T12:20:20.024277+00:00",  # TODO change to basic_dag id
        # upload_manifest=False,
        # upload_run_results=True,
        # upload_sources_json=True,
    )
    def dbt_source_freshness():
        return "dbt source freshness" # This should generate run_results and sources_json
        # return "dbt ls"

    @task.datacoves_dbt(connection_id="main", upload_static_artifacts=False)
    def dbt_build():
        return "dbt seed && dbt build -s stg_personal_loans+"

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
                # dbt run -s 1+state:error+

            """
            dbt run -s state:error+
            personal_loads- > model2
            """

        

    dbt_source_freshness() >> dbt_build() >> download_artifacts(expected_files=["run_results.json", "sources.json", "breaking_file.txt"])
