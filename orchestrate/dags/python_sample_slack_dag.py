from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from callbacks.slack_messages import inform_success

DATACOVES_INTEGRATION_NAME = "DATACOVES_SLACK"


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

default_args = {
    'owner': 'airflow',
    'email': 'some_user@example.com',
    'email_on_failure': True,
    'description': "Sample python dag with Slack notification",
}

with DAG(
    dag_id="python_sample_slack_dag",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["version_25"],
    description="Sample python dag dbt run",
    schedule_interval="0 0 1 */12 *",
    on_success_callback=run_inform_success,
    on_failure_callback=run_inform_failure,
) as dag:

    successful_task = BashOperator(
        task_id = "successful_task",
        bash_command = "echo SUCCESS"
    )

    # failing_task = BashOperator(
    #     task_id = "failing_task",
    #     bash_command = "some_non_existant_command"
    # )

    # runs failing task
    # successful_task >> failing_task

    successful_task
