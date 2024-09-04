import datetime

from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator

env = {
    "VAR_1": "",
    "VAR_2": "",
    "VAR_3": "",
    "VAR_4": "",
    "VAR_5": "",
}


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Bruno Antonellini",
        "email": "bruno@datacoves.com",
        "email_on_failure": True,
    },
    description="Sample DAG for dbt build",
    schedule_interval="0 0 1 */12 *",
    tags=["version_3"],
    catchup=False,
)
def empty_env_dag():
    run_dbt = DatacovesBashOperator(
        task_id="run_dbt", bash_command="echo $EMPTY_VAR", env=env, append_env=True
    )
    run_dbt


dag = empty_env_dag()
