import datetime

from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator


@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
        "retries": 3,
    },
    description="Sample DAG for dbt build",
    schedule="0 0 1 */12 *",
    tags=["transform"],
    catchup=False,
)
def yaml_dbt_dag():
    run_dbt = DatacovesDbtOperator(
        task_id="run_dbt", bash_command="dbt run -s personal_loans"
    )


dag = yaml_dbt_dag()
