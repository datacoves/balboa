from airflow import DAG
from airflow.decorators import dag, task
from operators.datacoves.dbt import DatacovesDbtOperator
from pendulum import datetime

# Only here for reference, this is automatically activated by Datacoves Operator
DATACOVES_VIRTIAL_ENV = "/opt/datacoves/virtualenvs/main/bin/activate"


@dag(
    default_args={
        "start_date": datetime(2022, 10, 10),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    catchup=False,
    tags=["version_1"],
    description="Datacoves Sample dag",
    # This is a regular CRON schedule. Helpful resources
    # https://cron-ai.vercel.app/
    # https://crontab.guru/
    schedule_interval="0 0 1 */12 *",
)
def upload_manifest():

    # Calling dbt commands
    dbt_debug_upload = DatacovesDbtOperator(
        task_id="run_dbt_task", bash_command="dbt ls", upload_manifest=True
    )
    dbt_debug_upload


# Invoke Dag
dag = upload_manifest()
