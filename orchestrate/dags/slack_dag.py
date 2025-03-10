from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.slack.notifications.slack import send_slack_notification

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
    tags=["version_1", "slack_notification"],
    catchup=False,
    on_success_callback=send_slack_notification(
        text="The DAG {{ dag.dag_id }} succeeded", slack_conn_id="AlejandroSlack"
    ),
    on_failure_callback=send_slack_notification(
        text="The DAG {{ dag.dag_id }} failed", slack_conn_id="AlejandroSlack"
    ),
)
def slack_notification_dag():

    @task.datacoves_dbt(connection_id="main")
    def transform():
        return "dbt debug"

    transform()

# Invoke DAG
dag = slack_notification_dag()
