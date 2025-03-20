import datetime

from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "John Doe",
        "email": "john@example.com",
        "email_on_failure": True,
    },
    description="Daily dbt run",
    schedule_interval="0 12 * * *",
    tags=["version_1"],
    catchup=False,
)
def daily_run():
    run_dbt = DatacovesDbtOperator(
        task_id="run_dbt", bash_command="dbt source freshness && dbt build"
    )


dag = daily_run()
