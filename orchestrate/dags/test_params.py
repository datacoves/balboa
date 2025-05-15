"""
## Sample DAG showing how to run dbt
This DAG shows how to run a dbt task
"""

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils
from operators.datacoves.bash import DatacovesBashOperator


params = {
    "param1": "Hello World",
}

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args=datacoves_utils.set_default_args(
        owner = "Bruno Antonellini",
        owner_email = "bruno@datacoves.com"
    ),

    schedule = datacoves_utils.set_schedule("0 0 1 */12 *"),
    description="Sample DAG demonstrating how to run dlt in airflow",
    tags=["test", "bruno"],
)
def test_params():
    bash_task = DatacovesBashOperator(
        task_id="dbt_task",
        bash_command="echo '{{params.param1}}'",
        params=params,

    )
    bash_task

test_params()
