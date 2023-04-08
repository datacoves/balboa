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
    message = ''
    theme_color = ''

    logs_url = f"{AIRFLOW_BASE_URL}/log?dag_id={dag_id}&task_id={task_id}&execution_date={timestamp}"

    # configure message based on run state
    if context['dag_run'].state == 'success':
        message = f"`{dag_id}` has completed successfully"
        theme_color = "00FF00"
    elif context['dag_run'].state == 'failed':
        message = f"`{dag_id}` has failed on task: `{task_id}`"
        theme_color = "FF0000"
    else:
        message = context['dag_run'].state
        theme_color = "0000FF"

    ms_teams_notification = MSTeamsWebhookOperator(
        task_id = "msteams_notify_failure",
        trigger_rule = "all_done",
        button_text = "View log",
        button_url = logs_url,
        message = message,
        theme_color = theme_color,
        http_conn_id = DATACOVES_INTEGRATION_NAME
    )

    ms_teams_notification.execute(context)

def set_task_callbacks(dag, on_success_callback, on_failure_callback):
    for task:
        # IMPORTANT: it's the reference to the method, do not call() it
        task.on_success, task.on_success_callback = ms_teams_send_logs
        task.on_failure_callback = ms_teams_send_logs

    # 'on_success_callback': ms_teams_send_logs,
    # 'on_failure_callback': ms_teams_send_logs

default_args = {
    'owner': 'airflow',
    'email': 'gomezn@datacoves.com',
    'email_on_failure': True,
    'description': "Sample python dag with MS Teams notification",
}

with DAG(
    dag_id = "python_sample_teams_dag",
    default_args = default_args,
    start_date = datetime(2021, 1, 1),
    catchup = False,
    tags = ["version_11"],
    description = "Sample python dag dbt run",
    schedule_interval = "0 0 1 */12 *",
    on_success_callback = ms_teams_send_logs,
    on_failure_callback = ms_teams_send_logs
) as dag:

    successful_task = BashOperator(
        task_id = "successful_task",
        bash_command = "echo SUCCESS"
    )

    # Call the helper function to set the callbacks for all tasks
    set_task_callbacks(dag, ms_teams_send_logs, ms_teams_send_logs)

    successful_task
