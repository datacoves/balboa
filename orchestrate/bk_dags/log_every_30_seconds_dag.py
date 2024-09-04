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
def test_logging_dag_20min():
    @task(task_id="log_every_30_seconds")
    def log_every_30_seconds(ds=None, **kwargs):
        """Print a 'LOGGING' message every 30 seconds for 20 minutes"""
        for i in range(40):
            print("LOGGING")
            time.sleep(30)

        return "Finished logging"

    run_this = log_every_30_seconds()


# Invoke Dag
dag = test_logging_dag_20min()
