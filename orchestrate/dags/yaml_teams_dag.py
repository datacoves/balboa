import datetime

from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    description="Sample DAG with MS Teams notification",
    schedule_interval="0 0 1 */12 *",
    tags=["version_2", "ms_teams_notification", "blue_green"],
    catchup=False,
    custom_callbacks={
        "on_success_callback": {
            "module": "callbacks.microsoft_teams",
            "callable": "inform_success",
            "args": {"connection_id": "DATACOVES_MS_TEAMS", "color": "0000FF"},
        },
        "on_failure_callback": {
            "module": "callbacks.microsoft_teams",
            "callable": "inform_failure",
            "args": {"connection_id": "DATACOVES_MS_TEAMS", "color": "9900FF"},
        },
    },
)
def yaml_teams_dag():
    transform = DatacovesDbtOperator(
        task_id="transform", bash_command="dbt run -s personal_loans"
    )


dag = yaml_teams_dag()
