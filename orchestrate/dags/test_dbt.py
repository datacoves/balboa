from datetime import datetime, timedelta
from airflow.decorators import dag, task


@dag(
    default_args={
        "owner": "Alejandro",
        "depends_on_past": False,
        "start_date": datetime.today() - timedelta(days=1),
        "email": "alejandro@datacoves.com",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    description="DAG for testing dbt debug.",
    schedule="23 20 * * 1-5",
    tags=["version_6"],
    catchup=False,
)
def dag_dbt_debug():

    @task.datacoves_dbt(
        connection_id="main"
    )
    def run_dbt():
        return "dbt debug && dbt run --select stg__airbyte_raw_zip_coordinates"

    run_dbt()


dag_dbt_debug()
