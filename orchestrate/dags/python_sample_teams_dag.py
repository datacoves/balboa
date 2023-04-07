from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s

import os
import urllib.parse
from ms_teams.ms_teams_webhook_operator import MSTeamsWebhookOperator

AIRFLOW_BASE_URL = os.environ.get("AIRFLOW__WEBSERVER__BASE_URL")
DATACOVES_INTEGRATION_NAME = "DATACOVES_MS_TEAMS"

def ms_teams_send_logs(context):
    dag_id = context["dag_run"].dag_id
    task_id = context["task_instance"].task_id
    context["task_instance"].xcom_push(key=dag_id, value=True)
    timestamp = urllib.parse.quote(context['ts'])

    logs_url = f"{AIRFLOW_BASE_URL}/log?dag_id={dag_id}&task_id={task_id}&execution_date={timestamp}"
    ms_teams_notification = MSTeamsWebhookOperator(
        task_id = "msteams_notify_failure",
        trigger_rule = "all_done",
        message = "`{}` has failed on task: `{}`".format(dag_id, task_id),
        button_text = "View log",
        button_url = logs_url,
        theme_color = "FF0000",
        http_conn_id = DATACOVES_INTEGRATION_NAME
    )

    ms_teams_notification.execute(context)

default_args = {
    'owner': 'airflow',
    'email': 'gomezn@datacoves.com',
    'email_on_failure': True,
    'description': "Sample python dag with MS Teams notification",
    # IMPORTANT: it's the reference to the method, do not call() it
    'on_failure_callback': ms_teams_send_logs
}


with DAG(
    dag_id = "python_sample_teams_dag",
    default_args = default_args,
    start_date = datetime(2021, 1, 1),
    catchup = False,
    tags = ["version_2"],
    description = "Sample python dag dbt run",
    schedule_interval = "0 0 1 */12 *"
) as dag:

    successful_task = BashOperator(
        task_id = "successful_task",
        bash_command = "echo SUCCESS"
    )

    failing_task = BashOperator(
        task_id = 'failing_task',
        bash_command = "some_non_existant_command"
    )

    successful_task >> failing_task
