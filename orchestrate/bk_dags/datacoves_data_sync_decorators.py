import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from operators.datacoves.bash import DatacovesBashOperator


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Bruno Antonellini",
        "email": "bruno@datacoves.com",
        "email_on_failure": False,
    },
    description="Sample DAG for dbt build",
    schedule_interval="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def data_sync_decorators():
    @task.datacoves_data_sync_snowflake(
        destination_schema="test_schema_nov_19",
        # additional_tables=["additional_table_1", "additional_table_2"],
        service_connection_name="main",
    )
    def data_sync_snowflake():
        pass

    data_sync_snowflake()


dag = data_sync_decorators()
