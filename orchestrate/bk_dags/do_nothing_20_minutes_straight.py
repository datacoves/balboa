import time

from airflow.decorators import dag, task
from pendulum import datetime

# Only here for reference, this is automatically activated by Datacoves Operator
DATACOVES_VIRTUAL_ENV = "/opt/datacoves/virtualenvs/main/bin/activate"


@dag(
    default_args={
        "start_date": datetime(2022, 10, 10),
        "owner": "Bruno Antonellini",
        "email": "nothing@nothing.com",
        "email_on_failure": True,
    },
    catchup=False,
    tags=["version_1", "testing"],
    description="Datacoves Sample dag",
    # This is a regular CRON schedule. Helpful resources
    # https://cron-ai.vercel.app/
    # https://crontab.guru/
    schedule_interval="0 0 1 */12 *",
)
def do_nothing_20_minutes_straight():
    @task(task_id="log_every_30_seconds")
    def do_nothing_20_minutes(ds=None, **kwargs):
        """Print a 'LOGGING' message every 30 seconds for 20 minutes"""
        time.sleep(1200)
        return

    run_this = do_nothing_20_minutes()


# Invoke Dag
dag = do_nothing_20_minutes_straight()
