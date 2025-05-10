"""
## Sample DAG using variables
This DAG is a sample using the Datacoves decorators with variable from AWS.
"""

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.secrets_manager import SecretsManagerHook

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args={
        "start_date": datetime(2024, 1, 1),
        "owner": "Noel Gomez",
        "email": "noel@example.com",
        "email_on_failure": True,
        "retries": 3,
    },
    tags=["sample"],
    description="Testing task decorators",
    schedule_interval="0 0 1 */12 *",
)
def variable_usage():

    @task.datacoves_bash
    def aws_var():
        secrets_manager_hook = SecretsManagerHook(aws_conn_id='aws_secrets_manager')
        var = secrets_manager_hook.get_secret("airflow/variables/aws_ngtest")
        return f"export MY_VAR={var} && echo $MY_VAR"

    aws_var()

variable_usage()
