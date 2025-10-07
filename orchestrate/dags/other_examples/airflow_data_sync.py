"""
## Airflow dbt Sync Example
This DAG shows how to copy the Airflow Database to Snowflake
"""

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils
from airflow.hooks.base import BaseHook

kp_conn = BaseHook.get_connection("main_key_pair")


@dag(
    doc_md=__doc__,
    catchup=False,
    default_args=datacoves_utils.set_default_args(
        owner="Bruno", owner_email="bruno@example.com"
    ),
    description="Sample DAG to synchronize the Airflow database",
    schedule=datacoves_utils.set_schedule("0 0 1 */12 *"),
    tags=["extract_and_load", "version_2"],
)
def airflow_data_sync():
    @task.datacoves_airflow_db_sync(
        db_type="snowflake",
        destination_schema="airflow_dev",
        connection_id="main_key_pair",
        # additional_tables=["additional_table_1", "additional_table_2"]
    )
    def sync_airflow_db():
        pass

    sync_airflow_db()


airflow_data_sync()
