"""## Datacoves Bash Operator DAG
This DAG is a sample using the Datacoves Airflow Operators"""

from airflow.decorators import dag, task
from airflow.models import Variable
from operators.datacoves.bash import DatacovesBashOperator
from operators.datacoves.dbt import DatacovesDbtOperator
from pendulum import datetime

# Only here for reference, this is automatically activated by Datacoves Operator
DATACOVES_VIRTUAL_ENV = "/opt/datacoves/virtualenvs/main/bin/activate"


@dag(
    default_args={
        "start_date": datetime(2022, 10, 10),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    catchup=False,
    tags=["version_6"],
    description="Steve Test dag",
    # This is a regular CRON schedule. Helpful resources
    # https://cron-ai.vercel.app/
    # https://crontab.guru/
    schedule_interval="0 0 1 */12 *",
)
def stevetest_dag():
    @task.datacoves_dbt(
        connection_id="main"
    )
    def dbt_test() -> str:
        return "dbt debug"

    @task.datacoves_dbt(
        connection_id="bigquery"
    )
    def dbt_test_bigquery() -> str:
        return "dbt debug"

    @task.datacoves_dbt(
        connection_id="redshift"
    )
    def dbt_test_redshift() -> str:
        return "dbt debug"

    @task.datacoves_dbt(
        connection_id="databricks"
    )
    def dbt_test_databricks() -> str:
        return "dbt debug"

    print(dbt_test())
    print(dbt_test_bigquery())
    print(dbt_test_redshift())
    print(dbt_test_databricks())

# Invoke Dag
dag = stevetest_dag()
dag.doc_md = __doc__
