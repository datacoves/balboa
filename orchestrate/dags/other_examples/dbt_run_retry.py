"""## Datacoves Operators Sample DAG
This DAG is a sample using the Datacoves Airflow Operators"""

from airflow.decorators import dag, task, task_group
from operators.datacoves.dbt import DatacovesDbtOperator
from airflow.operators.dummy import DummyOperator
from pendulum import datetime
import os

@dag(
    doc_md = __doc__,
    default_args = {
        "start_date": datetime(2022, 10, 10),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
        "retries": 0,
    },
    catchup = False,
    tags = ["python_script"],
    description = "Datacoves Sample dag",

    # This is a regular CRON schedule. Helpful resources
    # https://cron-ai.vercel.app/
    # https://crontab.guru/
    schedule = "0 0 1 */12 *",
)
def dbt_run_retry_dag():

    @task.branch
    def is_this_retry():
        target_dir = os.path.join(os.environ.get('DATACOVES__DBT_HOME'), 'target')
        file_path = os.path.join(target_dir, 'run_results.json')

        print("CHECKING IF THIS DAG RAN")
        print(f"\nContents of {target_dir} directory:")

        # Check if target directory exists
        if os.path.exists(target_dir):
            try:
                # List all files in the directory with their details
                with os.scandir(target_dir) as entries:
                    for entry in entries:
                        # Get file stats
                        stats = entry.stat()
                        size = stats.st_size
                        # Print file name and size
                        print(f"- {entry.name} ({size} bytes)")
            except Exception as e:
                print(f"Error reading directory: {e}")
        else:
            print(f"Directory {target_dir} does not exist!")

        # Original branching logic
        if os.path.exists(file_path):
            print("#### IT DID ####")
            return 'dbt_and_python.dbt_retry'
        else:
            print("#### IT DID NOT ####")
            return 'dbt_and_python.dbt_run'



    run_results_exists = is_this_retry()

    @task_group(group_id="dbt_and_python", tooltip="This is a group that runs dbt then a custom python file")
    def dbt_run_or_retry():
        dbt_run = DatacovesDbtOperator(
            task_id = "dbt_run",
            # bash_command = "dbt debug"
            # bash_command = "dbt debug --profiles-dir ../.vscode",
            bash_command = "dbt run -s jhu_covid_19+ --profiles-dir ../.vscode",
        )

        dbt_retry = DatacovesDbtOperator(
            task_id = "dbt_retry",
            # bash_command = "dbt debug"
            # bash_command = "dbt debug --profiles-dir ../.vscode",
            bash_command = "dbt retry --profiles-dir ../.vscode",
        )

        final_task = DummyOperator(
            task_id='end',
            trigger_rule='one_success'
        )

        # Define task dependencies
        dbt_run.set_upstream(run_results_exists)
        dbt_retry.set_upstream(run_results_exists)
        final_task.set_upstream([dbt_run, dbt_retry])


    dbt_run_or_retry()

# Invoke Dag
dag = dbt_run_retry_dag()
