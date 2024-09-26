import os
from datetime import datetime, timedelta
from airflow.decorators import dag
# from airflow.operators.bash_operator import BashOperator
from operators.datacoves.dbt import DatacovesDbtOperator


@dag(
    default_args={
        "owner": "Kenvue",
        "depends_on_past": False,
        "start_date": datetime.today(),
        "email": "alejandro@datacoves.com",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    description="DAG for Refreshing CUBE_CM_AGG model.",
    schedule="18 20 * * 1-5",
    tags=["version_1"],
    catchup=False,
)
def dag_cube_cm_agg():

    """
    cube_cm_agg = BashOperator(
        task_id='hello_world_task',
        bash_command='python -c "print(\'Hello, world!\')"',
        dag=dag
    )
    """
    cube_cm_agg = DatacovesDbtOperator(
        task_id="cube_cm_agg",
        bash_command="dbt debug"
    )

    cube_cm_agg

dag = dag_cube_cm_agg()
