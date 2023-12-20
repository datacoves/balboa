import datetime

from airflow.decorators import dag
from callbacks.slack_messages import inform_failure, inform_success
from kubernetes.client import models as k8s
from operators.datacoves.bash import DatacovesBashOperator


def run_inform_success(context):
    inform_success(context, connection_id="DATACOVES_SLACK", color="0000FF")


def run_inform_failure(context):
    inform_failure(context, connection_id="DATACOVES_SLACK", color="9900FF")


TRANSFORM_CONFIG = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="transform",
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
    tags=["version_1", "slack_notification", "blue_green"],
    catchup=False,
    on_success_callback=run_inform_success,
    on_failure_callback=run_inform_failure,
)
def yaml_slack_dag():
    transform = DatacovesBashOperator(
        task_id="transform",
        bash_command="$DATACOVES__REPO_PATH/automate/blue_green_run.py -s 'tag:daily_run_airbyte+ tag:daily_run_fivetran+ -t prd'",
        executor_config=TRANSFORM_CONFIG,
    )
    marketing_automation = DatacovesBashOperator(
        task_id="marketing_automation",
        bash_command="echo 'send data to marketing tool'",
    )
    marketing_automation.set_upstream([transform])
    update_catalog = DatacovesBashOperator(
        task_id="update_catalog", bash_command="echo 'refresh data catalog'"
    )
    update_catalog.set_upstream([transform])
    failing_task = DatacovesBashOperator(
        task_id="failing_task", bash_command="some_non_existant_command"
    )
    failing_task.set_upstream([transform])


dag = yaml_slack_dag()
