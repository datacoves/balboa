import datetime

from airflow.decorators import dag
from airflow.providers.slack.notifications.slack import send_slack_notification
from operators.datacoves.dbt import DatacovesDbtOperator


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    description="Sample DAG with Slack notification, custom image, and resource requests",
    schedule="0 0 1 */12 *",
    tags=["version_3", "slack_notification"],
    catchup=False,
    retries=3,
    on_success_callback=send_slack_notification(
        text="The DAG {{ dag.dag_id }} succeeded", channel="#general"
    ),
    on_failure_callback=send_slack_notification(
        text="The DAG {{ dag.dag_id }} failed", channel="#general"
    ),
)
def yaml_slack_dag():
    transform = DatacovesDbtOperator(
        task_id="transform", bash_command="dbt run -s personal_loans"
    )


dag = yaml_slack_dag()
