from fivetran_provider_async.operators import FivetranOperator
from fivetran_provider_async.sensors import FivetranSensor
from pendulum import datetime

from airflow.decorators import dag, task

FIVETRAN_CONNECTOR_ID = "gallop_maker"
GITHUB_REPOSITORY = "<your GitHub handle>/airflow-fivetran-tutorial"
TAG_NAME = "sync-metadata"


@dag(
    default_args={
        "start_date": datetime(2022, 10, 10),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    catchup=False,
    tags=["version_4"],
    description="Fivetran Astronomer dag",
    # This is a regular CRON schedule. Helpful resources
    # https://cron-ai.vercel.app/
    # https://crontab.guru/
    schedule_interval="0 0 1 */12 *",
)
def fivetran_astronomer_provider():
    # Calling dbt commands
    run_fivetran_sync = FivetranOperator(
        task_id="run_fivetran_sync",
        fivetran_conn_id="fivetran_conn",
        connector_id=FIVETRAN_CONNECTOR_ID,
        wait_for_completion=False,
    )

    fivetran_sensor = FivetranSensor(
        task_id="wait_for_fivetran_externally_scheduled_sync",
        poke_interval=5,
        fivetran_conn_id="fivetran_conn",
        connector_id=FIVETRAN_CONNECTOR_ID,
        completed_after_time="{{ data_interval_end + macros.timedelta(minutes=1) }}",
    )
    run_fivetran_sync >> fivetran_sensor


# Invoke Dag
dag = fivetran_astronomer_provider()
