import datetime

from airflow.decorators import dag
from airflow.providers.slack.notifications.slack import send_slack_notification
from kubernetes.client import models as k8s
from operators.datacoves.dbt import DatacovesDbtOperator

TRANSFORM_CONFIG = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    image="datacoves/airflow-pandas:latest",
                    resources=k8s.V1ResourceRequirements(
                        requests={"memory": "8Gi", "cpu": "1000m"}
                    ),
                )
            ]
        )
    ),
}


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    description="Sample DAG with Slack notification, custom image, and resource requests",
    schedule_interval="0 0 1 */12 *",
    tags=["version_2", "slack_notification", "blue_green"],
    catchup=False,
    on_success_callback=send_slack_notification(
        text="The DAG {{ dag.dag_id }} succeeded", channel="#general"
    ),
    on_failure_callback=send_slack_notification(
        text="The DAG {{ dag.dag_id }} failed", channel="#general"
    ),
)
def yaml_slack_dag():
    transform = DatacovesDbtOperator(
        task_id="transform",
        bash_command="dbt run -s personal_loans",
        executor_config=TRANSFORM_CONFIG,
    )


dag = yaml_slack_dag()
