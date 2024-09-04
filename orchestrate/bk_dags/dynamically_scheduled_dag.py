import datetime

from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator

from orchestrate.python_scripts.get_schedule import get_schedule


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Bruno Antonellini",
        "email": "bruno@datacoves.com",
        "email_on_failure": True,
    },
    description="Sample DAG for dbt build",
    schedule_interval=get_schedule("0 0 1 */12 *"),
    tags=["version_4"],
    catchup=False,
)
def dynamically_scheduled_dag():
    run_dbt = DatacovesBashOperator(task_id="run_dbt", bash_command="dbt ls")
    run_dbt


dag = dynamically_scheduled_dag()
