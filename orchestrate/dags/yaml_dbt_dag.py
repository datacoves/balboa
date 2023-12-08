import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@datacoves.com",
        "email_on_failure": True,
    },
    description="Sample DAG for dbt build",
    schedule_interval="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def yaml_dbt_dag():
    build_dbt = BashOperator(
        task_id="build_dbt",
        bash_command="source /opt/datacoves/virtualenvs/main/bin/activate && dbt-coves dbt -- ls --resource-type nonexistent",
    )


dag = yaml_dbt_dag()
