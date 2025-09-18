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

kp_conn = BaseHook.get_connection("main_key_pair")


def get_dbtcoves_env_vars(connection_id: str, prefix: str = "DATA_SYNC_SNOWFLAKE_"):
    """
    Retrieves connection details from Airflow and formats them as environment variables.

    Args:
        connection_id (str): The Airflow connection ID.
        prefix (str): The prefix for the environment variables.
    Returns:
        dict: A dictionary of environment variables.
    """
    try:
        conn = BaseHook.get_connection(connection_id)
    except AirflowNotFoundException:
        logger.error(f"Connection ID '{connection_id}' not found.")
        return {}

    env_vars = {
        f"{prefix}USER": conn.login,
        f"{prefix}WAREHOUSE": conn.schema,
        f"{prefix}ACCOUNT": conn.extra_dejson.get("account", ""),
        f"{prefix}ROLE": conn.extra_dejson.get("role", ""),
        f"{prefix}DATABASE": conn.extra_dejson.get("database", ""),
        "DATA_SYNC_SOURCE_CONNECTION_STRING": os.environ.get(
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
        ),
    }
    if conn.extra_dejson.get("private_key"):
        env_vars[f"{prefix}PRIVATE_KEY"] = conn.extra_dejson.get("private_key")
    elif conn.password:
        env_vars[f"{prefix}PASSWORD"] = conn.password

    return env_vars


@dag(
    doc_md=__doc__,
    catchup=False,
    default_args=datacoves_utils.set_default_args(
        owner="Bruno", owner_email="bruno@example.com"
    ),
    description="Sample DAG to synchronize the Airflow database",
    schedule=datacoves_utils.set_schedule("0 0 1 */12 *"),
    tags=["extract_and_load", "version_1"],
)
def airflow_keypair_data_sync():
    @task.datacoves_bash(env=get_dbtcoves_env_vars("main_key_pair"))
    def sync_airflow_db_v2():
        return f"dbt-coves data-sync snowflake --source airflow_dev"

    sync_airflow_db_v2()


airflow_keypair_data_sync()
