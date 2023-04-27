from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from callbacks.microsoft_teams import inform_failure, inform_success

DATACOVES_INTEGRATION_NAME = "DATACOVES_MS_TEAMS"


def run_inform_success(context):
    inform_success(
        context,
        connection_id=DATACOVES_INTEGRATION_NAME,  # Only mandatory argument
        # message="Custom python success message",
        # color="FFFF00",
    )


def run_inform_failure(context):
    inform_failure(
        context,
        connection_id=DATACOVES_INTEGRATION_NAME,  # Only mandatory argument
        # message="Custom python failure message",
        # color="FF00FF",
    )


def set_task_callbacks(dag):  # Use if we want to set per-task callback
    for task in dag.tasks:
        task.on_success_callback = run_inform_success
        task.on_failure_callback = run_inform_failure


default_args = {
    "owner": "airflow",
    "email": "gomezn@datacoves.com",
    "email_on_failure": True,
    "description": "Sample python dag with MS Teams notification",
}

with DAG(
    dag_id="python_sample_teams_dag",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["version_17"],
    description="Sample python dag dbt run",
    schedule_interval="0 0 1 */12 *",
    on_success_callback=run_inform_success,
    on_failure_callback=run_inform_failure,
) as dag:
    successful_task = BashOperator(
        task_id="successful_task", bash_command="echo SUCCESS"
    )

    # failing_task = BashOperator(
    #     task_id = "failing_task",
    #     bash_command = "some_non_existant_command"
    # )

    # Call the helper function to set the callbacks for all tasks
    set_task_callbacks(dag)  # If we want to set per-task callback

    # runs failing task
    # successful_task >> failing_task

    successful_task
