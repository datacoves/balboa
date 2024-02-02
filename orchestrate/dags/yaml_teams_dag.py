import datetime

from airflow.decorators import dag
from callbacks.microsoft_teams import inform_failure, inform_success
from operators.datacoves.bash import DatacovesBashOperator
from operators.datacoves.dbt import DatacovesDbtOperator


def run_inform_success(context):
    inform_success(context, connection_id="DATACOVES_MS_TEAMS", color="0000FF")


def run_inform_failure(context):
    inform_failure(context, connection_id="DATACOVES_MS_TEAMS", color="9900FF")


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    description="Sample DAG with MS Teams notification",
    schedule_interval="0 0 1 */12 *",
    tags=["version_1", "ms_teams_notification", "blue_green"],
    catchup=False,
    on_success_callback=run_inform_success,
    on_failure_callback=run_inform_failure,
)
def yaml_teams_dag():
    transform = DatacovesDbtOperator(
        task_id="transform", bash_command="dbt run -s personal_loans"
    )
    failing_task = DatacovesBashOperator(
        task_id="failing_task", bash_command="some_non_existant_command"
    )
    failing_task.set_upstream([transform])


dag = yaml_teams_dag()
