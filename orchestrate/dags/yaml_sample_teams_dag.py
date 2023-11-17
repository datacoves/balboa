import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from callbacks.microsoft_teams import inform_failure, inform_success


def run_inform_success(context):
    inform_success(context, connection_id="DATACOVES_MS_TEAMS")


def run_inform_failure(context):
    inform_failure(context, connection_id="DATACOVES_MS_TEAMS")


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "airflow",
        "email": "gomezn@datacoves.com",
        "email_on_failure": True,
    },
    description="Sample yaml dag dbt run",
    schedule_interval="0 0 1 */12 *",
    tags=["version_5"],
    catchup=False,
    on_success_callback=run_inform_success,
    on_failure_callback=run_inform_failure,
)
def yaml_sample_teams_dag():
    successful_task = BashOperator(
        task_id="successful_task", bash_command="echo SUCCESS!"
    )
    failing_task = BashOperator(
        task_id="failing_task", bash_command="some_non_existant_command"
    )
    failing_task.set_upstream([successful_task])
    successful_task


dag = yaml_sample_teams_dag()
