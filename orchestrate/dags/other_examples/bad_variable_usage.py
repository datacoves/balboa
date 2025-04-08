import datetime
from airflow.decorators import dag, task, task_group
from airflow.models import Variable


# ❌ BAD PRACTICE: Fetching a variable at the top level
# This will cause Airflow to query for this variable on EVERY DAG PARSE (every 30 seconds),
# which can be costly when using an external secrets manager (e.g., AWS Secrets Manager).
bad_used_variable = Variable.get("bad_used_variable", "default_value")

@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
        "retries": 3
    },
    description="Sample DAG demonstrating bad variable usage",
    schedule="0 0 1 */12 *",
    tags=["extract_and_load"],
    catchup=False,
)
def bad_variable_usage():

    @task.datacoves_bash(env={"BAD_VAR": bad_used_variable})  # ✅ Passing the bad variable to the task
    def print_var():
        return "echo $BAD_VAR"

    print_var()

# Invoke DAG
bad_variable_usage()
