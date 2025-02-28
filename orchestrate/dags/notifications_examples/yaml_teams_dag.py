import datetime

from airflow.decorators import dag
from notifiers.datacoves.ms_teams import MSTeamsNotifier
from operators.datacoves.dbt import DatacovesDbtOperator


@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
        "retries": 3,
    },
    description="Sample DAG with MS Teams notification",
    schedule="0 0 1 */12 *",
    tags=["transform", "ms_teams_notification"],
    catchup=False,
    on_success_callback=MSTeamsNotifier(
        connection_id="DATACOVES_MS_TEAMS", theme_color="0000FF"
    ),
    on_failure_callback=MSTeamsNotifier(
        connection_id="DATACOVES_MS_TEAMS", theme_color="9900FF"
    ),
)
def yaml_teams_dag():
    transform = DatacovesDbtOperator(
        task_id="transform", bash_command="dbt run -s personal_loans"
    )


dag = yaml_teams_dag()
