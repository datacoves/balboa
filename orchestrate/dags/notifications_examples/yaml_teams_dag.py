import datetime
from airflow.decorators import dag, task
from notifiers.datacoves.ms_teams import MSTeamsNotifier

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

    @task.datacoves_dbt(connection_id="main")  
    def transform():
        return "dbt run -s personal_loans"

    transform() 

# Invoke DAG
dag = yaml_teams_dag()
