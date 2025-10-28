from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": "DL-JRDUS-RDI-WORKFLOW-NOTIFICATION@ITS.JNJ.com",
    "retries": 2,
}

@dag(
    doc_md = __doc__,
    catchup = False,
    start_date=datetime(2024, 7, 7),

    default_args = default_args,
    schedule = '45 6 * * 1-5',

    description = 'To load the L3 models for product cmc datya package  daily',
    tags = ['transform'],
    dag_id = 'empty_task'
)
def general_job():

    # No pod created - runs in scheduler
    start = EmptyOperator(task_id='start')

    @task.datacoves_dbt(
        connection_id="main_key_pair"
    )
    def run_dbt(dbt_command):
        return dbt_command

    # No pod created - runs in scheduler
    end = EmptyOperator(task_id='end')

    # Specify task_id when calling the function
    task_a = run_dbt.override(task_id='task_a')("dbt debug")
    task_b = run_dbt.override(task_id='task_b')("dbt debug")

    start >> [task_a, task_b] >> end

general_job()
