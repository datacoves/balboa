from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.slack.notifications.slack_webhook import send_slack_webhook_notification

run_inform_success = send_slack_webhook_notification(
    text="The DAG {{ dag.dag_id }} succeeded", slack_webhook_conn_id="AlejandroSlack"
)

run_inform_failure = send_slack_webhook_notification(
    text="The DAG {{ dag.dag_id }} failed", slack_webhook_conn_id="AlejandroSlack"
)


@dag(
    default_args={
        "start_date": datetime.today() - timedelta(days=1),
        "owner": "Alejandro",
        "email": "alejandro@datacoves.com",
        "email_on_failure": True,
        "retries": 0,
    },
    description="Sample DAG with Slack notification, custom image, and resource requests",
    schedule="0 0 1 */12 *",
    tags=["version_4", "slack_notification"],
    catchup=False,
    on_success_callback=[run_inform_success],
    on_failure_callback=[run_inform_failure],
)
def slack_notification_dag():

    @task.datacoves_dbt(connection_id="main")
    def transform():
        return "dbt debug"

    transform()

# Invoke DAG
dag = slack_notification_dag()
