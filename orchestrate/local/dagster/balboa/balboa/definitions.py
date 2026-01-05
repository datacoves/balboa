from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import balboa_dbt_assets
from .project import balboa_project
from .schedules import schedules

defs = Definitions(
    assets=[balboa_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=balboa_project, profiles_dir="/config/.dbt"),
    },
)