"""
## Retry dbt Example using Cosmos
This DAG is an example of using Cosmos to run a dbt DAG
"""
from datetime import datetime
# from pathlib import Path
import os

# from airflow import DAG  # Added this import
from orchestrate.utils import datacoves_utils

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode, RenderConfig
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping

DBT_HOME = os.getenv("DATACOVES__DBT_HOME")

# if not DBT_HOME:
#     # If DBT_HOME is not set, try REPO_PATH + transform
#     REPO_PATH = os.getenv("DATACOVES__REPO_PATH")
#     if REPO_PATH:
#         DBT_ROOT_PATH = Path(REPO_PATH) / "transform"
#     else:
#         # Fallback path
#         DBT_ROOT_PATH = Path("/tmp/dbt")
# else:
#     DBT_ROOT_PATH = Path(DBT_HOME)

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakePrivateKeyPemProfileMapping(
        conn_id='main_key_pair',
    ),
)

VIRTUALENV = "/opt/datacoves/virtualenvs/main"

retry_dbt_failure_cosmos = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_HOME,
    ),
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL,
        dbt_executable_path=f"{VIRTUALENV}/bin/dbt",
    ),
    render_config=RenderConfig(
        select=["stg_us_population+", "stg_personal_loans+"],
    ),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,
        "full_refresh": True,
    },
    schedule=datacoves_utils.set_schedule("0 0 1 */12 *"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="retry_dbt_failure_cosmos",
    default_args={"retries": 2},
    description="Sample DAG demonstrating how to run the dbt models that fail",
    tags=["transform", "retry"],
)
