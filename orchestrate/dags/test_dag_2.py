import os
from datetime import datetime, timedelta
from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator
from utils.test import test


@dag(
    default_args={
        "owner": "Datacoves",
        "depends_on_past": False,
        "start_date": datetime.today() - timedelta(days=1),
        "email": "alejandro@datacoves.com",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    description="DAG for Refreshing CUBE_CM_AGG model.",
    schedule="23 20 * * 1-5",
    tags=["version_1"],
    catchup=False,
)
def test_dag_import_utils():
    test()
    cube_cm_agg = DatacovesDbtOperator(
        task_id="cube_cm_agg",
        bash_command="dbt debug"
    )

    cube_cm_agg

dag = dag_cube_cm_agg()
