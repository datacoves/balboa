"""
## Sample DAG showing templated overrides on the dbt decorator
This DAG shows how to parameterize the `@task.datacoves_dbt` `overrides` kwarg
at trigger time using Jinja templates and DAG run `params`, instead of having
to hardcode the schema/role/warehouse or fall back to an Airflow Variable.
"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from orchestrate.utils import datacoves_utils

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args = datacoves_utils.set_default_args(
        owner = "Ian",
        owner_email = "ian@example.com"
    ),

    description="Sample DAG showing templated overrides on the dbt decorator",
    schedule = datacoves_utils.set_schedule(None),

    params = {
        "dbt_command": Param(
            "dbt build --select models/l1/schema_name+",
            type="string",
            description="dbt command to run",
        ),
        "schema_override": Param(
            "MANUAL_REFRESH",
            type="string",
            description="Schema to build into for this run",
        ),
    },

    tags=["transform", "parameters"],
)
def dbt_templated_overrides():

    @task.datacoves_dbt(
        connection_id="main_key_pair",
        overrides={"schema": "{{ params.schema_override }}"},
    )
    def run_dbt(dbt_command):
        return dbt_command

    run_dbt("{{ params.dbt_command }}")

dbt_templated_overrides()
