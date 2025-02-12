from airflow.decorators import dag, task
from pendulum import datetime
from orchestrate.python_scripts.get_schedule import get_schedule

@dag(
    default_args={
        "start_date": datetime(2022, 10, 10),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    catchup=False,
    tags=["version_8"],
    description="Datacoves Sample DAG",
    schedule=get_schedule('0 1 * * *'),  # Replace with desired schedule
)
def datacoves_sample_dag():

    @task.datacoves_dbt(connection_id="main")
    def run_dbt_task():
        return "dbt debug"

    run_dbt_task()

datacoves_sample_dag()
