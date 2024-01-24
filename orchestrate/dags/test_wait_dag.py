import datetime
from time import sleep
from airflow.decorators import dag, task


@dag(
    default_args={"start_date": "2021-01"},
    description="Wait Run",
    schedule_interval="0 0 1 */12 *",
    tags=["version_3"],
    catchup=False,
)
def wait_dag():
    @task(task_id="wait_10_min")
    def wait_10_min(**kwargs):
        """Print the Airflow context and ds variable from the context."""
        sleep(600)
        return 'Waited for 10min'

    run_this = wait_10_min()


dag = wait_dag()
