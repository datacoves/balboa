import os

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

from kubernetes.client import models as k8s
import urllib.parse
from ms_teams.ms_teams_webhook_operator import MSTeamsWebhookOperator


AIRFLOW_BASE_URL = os.environ.get("AIRFLOW__WEBSERVER__BASE_URL")

def ms_teams_send_logs(context):
    dag_id = context["dag_run"].dag_id
    task_id = context["task_instance"].task_id
    context["task_instance"].xcom_push(key=dag_id, value=True)
    timestamp = urllib.parse.quote(context['ts'])

    logs_url = f"{AIRFLOW_BASE_URL}/log?dag_id={dag_id}&task_id={task_id}&execution_date={timestamp}"
    ms_teams_notification = MSTeamsWebhookOperator(
        task_id="msteams_notify_failure", trigger_rule="all_done",
        message="`{}` has failed on task: `{}`".format(dag_id, task_id),
        button_text="View log", button_url=logs_url,
        theme_color="FF0000", http_conn_id='MSTEAMS_NOTIFICATIONS')

    ms_teams_notification.execute(context)

default_args = {
    'owner' : 'airflow',
    'description' : 'a test dag',
    'on_failure_callback': ms_teams_send_logs # IMPORTANT: it's the reference to the method, do not call() it
}

with DAG(
    dag_id="test_pandas",
    default_args=default_args,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["sample_tag"],
) as dag:
    executor_config_template = {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base", image="datacoves/airflow-pandas:latest"
                    )
                ]
            )
        ),
    }

    fail = BashOperator(
        task_id='failing',
        bash_command="dates"
    )

    fail
