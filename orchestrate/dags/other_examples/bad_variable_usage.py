"""
## Top Level Code Example
This DAG illustrates a DAG that has top level code which is bad
"""

from airflow.decorators import dag, task
from airflow.models import Variable
from orchestrate.utils import datacoves_utils


# ❌ BAD PRACTICE: Fetching a variable at the top level
# This will cause Airflow to query for this variable on EVERY DAG PARSE (every 30 seconds),
# which can be costly when using an external secrets manager (e.g., AWS Secrets Manager).
bad_used_variable = Variable.get("bad_used_variable", "default_value")

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args = datacoves_utils.set_default_args(
        owner = "Noel Gomez",
        owner_email = "noel@example.com"
    ),

    description="Sample DAG demonstrating bad variable usage (top level code)",
    schedule = datacoves_utils.set_schedule("0 0 1 */12 *"),
    tags = ["sample"],
)

def bad_variable_usage():

    @task.datacoves_bash(env={"BAD_VAR": bad_used_variable})  # ✅ Passing the bad variable to the task
    def print_var():
        return "echo $BAD_VAR"

    print_var()

    @task.datacoves_bash
    def aws_var():
        var = Variable.get("aws_ngtest", "default_value")
        return f"export MY_VAR={var} && echo $MY_VAR"

    aws_var()

bad_variable_usage()
