import datetime

from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator


@dag(
    default_args={"start_date": "2021-01"},
    description="Echo DATACOVES__REPO_PATH",
    schedule_interval="0 0 1 */12 *",
    tags=["version_2"],
    catchup=False,
)
def test_repo_path():
    extract_and_load_dlt = DatacovesBashOperator(
        task_id="extract_and_load_dlt",
        bash_command=" ls -lha $DATACOVES__REPO_PATH",
    )


dag = test_repo_path()
