import datetime

from operators.datacoves.bash import DatacovesBashOperator

from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    description="Sample DAG for dbt build",
    schedule_interval="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def datacoves_decorators():
    @task.datacoves_bash
    def echo_datacoves() -> str:
        var = Variable.get("test")
        return f"echo 'Hello {var}!'"

    bash = echo_datacoves()

    @task.datacoves_dbt
    def dbt_ls() -> str:
        var = Variable.get("test")
        return f"dbt ls && echo {var}"

    dbt = dbt_ls()

    run_2 = DatacovesBashOperator(
        task_id="echo_datacoves",
        bash_command="echo 'Something'",
    )
    bash >> dbt >> run_2


dag = datacoves_decorators()
