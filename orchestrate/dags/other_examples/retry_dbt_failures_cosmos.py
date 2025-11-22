"""
## Retry dbt Example using Cosmos
This DAG is an example of using Cosmos to run a dbt DAG
"""
from datetime import datetime
import os
from airflow.decorators import dag

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping
from orchestrate.utils import datacoves_utils

DBT_HOME = os.getenv("DATACOVES__DBT_HOME")
VIRTUALENV = "/opt/datacoves/virtualenvs/main"

profile_config = ProfileConfig(
    profile_name="default",
    target_name="prd",
    profile_mapping=SnowflakePrivateKeyPemProfileMapping(
        conn_id='main_key_pair',
    ),
)

@dag(
    start_date=datetime(2025, 1, 1),
    catchup=False,

    schedule = datacoves_utils.set_schedule("0 0 1 */12 *"),
    default_args={
        "retries": 2
    },
    tags=["transform", "retry"],
)
def retry_dbt_failures_cosmos():

    dbt_transformations = DbtTaskGroup(
        project_config=ProjectConfig(DBT_HOME),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{VIRTUALENV}/bin/dbt",
        ),
        render_config=RenderConfig(
            select=["+stg_us_population+", "+stg_personal_loans+"],
        ),
        operator_args={
            "full_refresh": False,
        },
    )

    dbt_transformations

retry_dbt_failures_cosmos()
