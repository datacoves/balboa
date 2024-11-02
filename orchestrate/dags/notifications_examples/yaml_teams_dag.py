import datetime

from airflow.decorators import dag
from notifiers.datacoves.ms_teams import MSTeamsNotifier
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
    tags=["version_3", "ms_teams_notification"],
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
