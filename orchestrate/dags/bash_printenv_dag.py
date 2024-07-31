from datetime import datetime, timedelta
from airflow.decorators import task

from airflow import DAG
from airflow.operators.bash import BashOperator
from time import sleep


def run_inform_success(context):
    print("run_inform_success")

def run_inform_failure(context):
    print("run_inform_failure")


default_args = {
    'owner': 'airflow',
    "description": "Sample dag bash operator",
    'email': 'amorera@datacoves.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="bash_printenv_dag",
    default_args=default_args,
    start_date = datetime(2024, 7, 10),
    catchup=False,
    tags=["version_2"],
    description="Sample dag",
    schedule_interval="*/10 * * * *",
    on_success_callback=run_inform_success,
    on_failure_callback=run_inform_failure,
) as dag:

    @task
    def eat_memory(**kwargs):
        a = []
        while True:
            a.append(' ' * 10**6)
            print(len(a))

        return {"msg": "Hello word!"}

    task_main = BashOperator(
        task_id = "task_main",
        bash_command = "sleep 60 && curl www.google.com && echo \"===========| LOG_LEVEL: $LOG_LEVEL |==========\""
    )

    task_main  # >> eat_memory()
