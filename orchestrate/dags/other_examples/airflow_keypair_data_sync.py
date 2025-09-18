"""
## Airflow dbt Sync Example
This DAG shows how to copy the Airflow Database to Snowflake
"""

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
from logging import Logger
import os

logger = Logger(__name__)

DEFAULT_AIRFLOW_TABLES = [
    "ab_permission",
    "ab_role",
    "ab_user",
    "dag",
    "dag_run",
    "dag_tag",
    "import_error",
    "job",
    "task_fail",
    "task_instance",
]


def get_dbtcoves_datasync_env_vars(
    connection_id: str, prefix: str = "DATA_SYNC_SNOWFLAKE_"
):
    conn = BaseHook.get_connection(connection_id)
    conn_extras = conn.extra_dejson

    env_vars = {
        f"{prefix}USER": conn.login,
        f"{prefix}WAREHOUSE": conn_extras.get("warehouse", ""),
        f"{prefix}ACCOUNT": conn_extras.get("account", ""),
        f"{prefix}ROLE": conn_extras.get("role", ""),
        f"{prefix}DATABASE": conn_extras.get("database", ""),
        "DATA_SYNC_SOURCE_CONNECTION_STRING": os.environ.get(
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
        ),
    }
    if conn_extras.get("private_key_content"):
        env_vars[f"{prefix}PRIVATE_KEY"] = conn_extras.get("private_key_content")
    elif conn.password:
        env_vars[f"{prefix}PASSWORD"] = conn.password

    return env_vars


@dag(
    doc_md=__doc__,
    catchup=False,
    default_args=datacoves_utils.set_default_args(
        owner="Bruno", owner_email="bruno@example.com"
    ),
    description="Sample DAG to synchronize the Airflow database using key-pair authentication",
    schedule=datacoves_utils.set_schedule("0 0 1 */12 *"),
    tags=["extract_and_load", "version_7"],
)
def airflow_keypair_data_sync():
    @task.datacoves_bash(env=get_dbtcoves_datasync_env_vars("main_key_pair"))
    def sync_airflow_db_v2():
        return f"dbt-coves data-sync snowflake --source airflow_dev --tables '{','.join(DEFAULT_AIRFLOW_TABLES)}'"

    sync_airflow_db_v2()


airflow_keypair_data_sync()
