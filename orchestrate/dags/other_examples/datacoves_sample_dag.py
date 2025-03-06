"""## Datacoves Operators Sample DAG
This DAG is a sample using the Datacoves Airflow Operators"""

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from pendulum import datetime

@dag(
    doc_md=__doc__,
    default_args={
        "start_date": datetime(2022, 10, 10),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
        "retries": 3,
    },
    catchup=False,
    tags=["python_script"],
    description="Datacoves Sample DAG",
    schedule="0 0 1 */12 *",
)
def datacoves_sample_dag():

    @task_group(
        group_id="dbt_and_python",
        tooltip="This is a group that runs dbt then a custom python file",
    )
    def dbt_and_python():
        """Task group containing dbt and Python script execution"""

        @task.datacoves_dbt(connection_id="main")  
        def run_dbt_task():
            return "dbt debug"

        @task.datacoves_bash(env = {
                "VAR1": Variable.get("my_var1", "default_value"),
                "VAR2": Variable.get("my_var2"),
                "VAR3": Variable.get("my_var3"),
            }
            )  # ✅ Fetching variables inside the task
        def run_python_script():
            return "python orchestrate/python_scripts/sample_script.py"

        dbt_task = run_dbt_task()
        python_task = run_python_script()

        dbt_task >> python_task 

    dbt_and_python()  

# Invoke DAG
dag = datacoves_sample_dag()
