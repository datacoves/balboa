"""
## Retry dbt Example using Cosmos
This DAG how to use
"""
from datetime import datetime
from pathlib import Path
import os

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode, RenderConfig
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping

DBT_ROOT_PATH = Path(os.getenv("DATACOVES__DBT_HOME"))

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakePrivateKeyPemProfileMapping(
    conn_id = 'main_key_pair',
),
)

VIRTUALENV = "/opt/datacoves/virtualenvs/main"

# [START local_example]
retry_dbt_failure_cosmos = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / '',
    ),
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.VIRTUALENV,
        virtualenv_dir=VIRTUALENV,
        dbt_executable_path=f"{VIRTUALENV}/bin/dbt",
    ),
    render_config=RenderConfig(
        select=["stg_us_population+", "stg_personal_loans+"],
    ),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True,  # used only in dbt commands that support this flag
    },

    # normal dag parameters
    schedule = datacoves_utils.set_schedule("0 0 1 */12 *"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="retry_dbt_failure_cosmos",
    default_args={"retries": 2},
    description="Sample DAG demonstrating how to run the dbt models that fail",
    tags=["transform"],
 )
 # [END local_example]

# @dag(
#     doc_md = __doc__,
#     catchup = False,

#     default_args=datacoves_utils.set_default_args(
#         owner = "Noel Gomez",
#         owner_email = "noel@example.com"
#     ),

#     schedule = datacoves_utils.set_schedule("0 0 1 */12 *"),
#     description="Sample DAG demonstrating how to run the dbt models that fail",
#     tags=["transform"],
# )
# def retry_dbt_failure_cosmos():

#     basic_cosmos_dag = DbtDag(
#         # dbt/cosmos-specific parameters
#         project_config=ProjectConfig(
#             DBT_ROOT_PATH,
#         ),
#     profile_config=profile_config,
#     operator_args={
#         "install_deps": True,  # install any necessary dependencies before running any dbt command
#         "full_refresh": True,  # used only in dbt commands that support this flag
#     },
#     # normal dag parameters
#     #  schedule_interval="@daily",
#     #  start_date=datetime(2023, 1, 1),
#     #  catchup=False,
#     #  dag_id="basic_cosmos_dag",
#     #  default_args={"retries": 2},
#     )


#     # @task.datacoves_dbt(
#     #     connection_id="main_key_pair",
#     #     dbt_api_enabled=True,
#     #     download_run_results=True,
#     # )
#     # def dbt_build(expected_files: list = []):
#     #     print(f"Expected Files Found?: =====> {expected_files}")
#     #     if expected_files:
#     #         return "dbt build -s result:error+ --state logs"
#     #     else:
#     #         return "dbt build -s stg_us_population+ stg_personal_loans+"


#     # dbt_build(expected_files=["run_results.json"])

# retry_dbt_failure_cosmos()


#  """
#  An example DAG that uses Cosmos to render a dbt project.
#  """

#  import os
#  from datetime import datetime
#  from pathlib import Path

#  from cosmos import DbtDag, ProjectConfig, ProfileConfig
#  from cosmos.profiles import PostgresUserPasswordProfileMapping

#  DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
#  DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

#  profile_config = ProfileConfig(
#      profile_name="default",
#      target_name="dev",
#      profile_mapping=PostgresUserPasswordProfileMapping(
#          conn_id="airflow_db",
#          profile_args={"schema": "public"},
#      ),
#  )

#  # [START local_example]
#  basic_cosmos_dag = DbtDag(
#      # dbt/cosmos-specific parameters
#      project_config=ProjectConfig(
#          DBT_ROOT_PATH / "jaffle_shop",
#      ),
#      profile_config=profile_config,
#      operator_args={
#          "install_deps": True,  # install any necessary dependencies before running any dbt command
#          "full_refresh": True,  # used only in dbt commands that support this flag
#      },
#      # normal dag parameters
#      schedule_interval="@daily",
#      start_date=datetime(2023, 1, 1),
#      catchup=False,
#      dag_id="basic_cosmos_dag",
#      default_args={"retries": 2},
#  )
#  # [END local_example]
