"""
Datacoves Operators/Decorators test dag. It ensures our operators and decorators work as expected.
"""

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils
from operators.datacoves.bash import DatacovesBashOperator
from operators.datacoves.dbt import DatacovesDbtOperator

@dag(
    doc_md=__doc__,
    catchup=False,
    default_args=datacoves_utils.set_default_args(
        owner="Bruno",
        owner_email="bruno@example.com"
    ),
    schedule=datacoves_utils.set_schedule("0 0 1 */12 *"),
    description="Datacoves test DAG to ensure our decorators/operators work",
    tags=["test", "v1"],
)
def datacoves_test():
    """
    Defaulting to a non-dbt command:
    - this way we can then use the datacoves_dbt decorator with connection_id
    - if using DatacovesDbtOperator to run dbt, we would need to set a new SSL Key every time:
                Env var required but not provided: 'DATACOVES__MAIN__PRIVATE_KEY'
    """
    test_dbt_operator = DatacovesDbtOperator(
        task_id="test_dbt_operator",
        bash_command="echo 'dbt command'",
    )

    test_bash_operator = DatacovesBashOperator(
        task_id="test_bash_operator", bash_command="echo '2nd step worked'"
    )

    @task.datacoves_dbt(connection_id="main")
    def test_dbt_decorator():
        return "dbt debug"

    @task.datacoves_bash()
    def test_bash_decorator():
        return "echo 'All worked yay!'"

    (
        test_dbt_operator
        >> test_bash_operator
        >> test_dbt_decorator()
        >> test_bash_decorator()
    )

datacoves_test()
