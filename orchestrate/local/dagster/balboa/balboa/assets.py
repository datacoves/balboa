from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import balboa_project


@dbt_assets(manifest=balboa_project.manifest_path)
def balboa_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    