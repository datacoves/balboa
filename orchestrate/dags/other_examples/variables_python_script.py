"""
## Sample DAG fetching Airflow Variables
This DAG shows how to fetch Airflow variables and store as environment variables to use in a python script.
"""

from airflow.decorators import dag, task, task_group
from airflow.models import Variable

from orchestrate.utils import datacoves_utils

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args = datacoves_utils.set_default_args(
        owner = "Noel Gomez",
        owner_email = "noel@example.com"
    ),

    description="Airflow Variables Sample DAG",
    schedule = datacoves_utils.set_schedule("0 0 1 */12 *"),
    tags=["python_script"],
)

def variables_python_script():

    @task_group(
        group_id="dbt_and_python",
        tooltip="Task group containing dbt and Python script execution",
    )
    def dbt_and_python():
        # First Task
        @task.datacoves_dbt(connection_id="main")
        def run_dbt_task():
            return "dbt debug"

        # Second Task
        @task.datacoves_bash(
            env = {
                "VAR1": "{{ var.value.my_var1 or 'default_value' }}",
                "VAR2": "{{ var.value.my_var2 }}",
                "VAR3": "{{ var.value.my_var3 }}",
            }
        )
        def run_python_script():
            return "python orchestrate/python_scripts/sample_script.py"

        # Invokes and sets the order of the tasks within the task group
        run_dbt_task() >> run_python_script()

    # Invokes the task group
    dbt_and_python()

# Invokes the DAG
variables_python_script()
