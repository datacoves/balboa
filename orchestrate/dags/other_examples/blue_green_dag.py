"""## Datacoves Blue-Green DAG"""

from airflow.decorators import dag, task
from pendulum import datetime

@dag(
    doc_md=__doc__,
    default_args={
        "start_date": datetime(2024, 1, 1),
        "owner": "Datacoves",
        "email": "bruno@example.com",
        "email_on_failure": True,
        "retries": 1,
    },
    catchup=False,
    tags=["transform"],
    description="Datacoves blue-green run",
    schedule="@daily",
)
def datacoves_bluegreen_dag():

    @task.datacoves_dbt(connection_id="main") 
    def blue_green_run():
        return "dbt-coves blue-green --dbt-selector '-s personal_loans'"

    blue_green_run()  

# Invoke Dag
dag = datacoves_bluegreen_dag()
