from datetime import datetime, timedelta
from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator


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
    tags=["version_4"],
    catchup=False,
)
def dag_dbt_debug():

    test_dbt = DatacovesDbtOperator(
        task_id="test_dbt",
        bash_command="dbt run --select stg__airbyte_raw_zip_coordinates"
    )

    test_dbt

dag = dag_dbt_debug()
