import datetime

from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    description="Sample DAG for dbt build",
    schedule_interval="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def datacoves_basic():
    @task.bash
    def echo_datacoves() -> str:
        var = Variable.get("test")
        return f"echo 'Hello {var}!'"

    echo_datacoves()


dag = datacoves_basic()
