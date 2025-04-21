from datetime import datetime, timedelta
from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator


@dag(
    default_args={
        "owner": "Alejandro",
        "depends_on_past": False,
        "start_date": datetime.today() - timedelta(days=1),
        "email": "alejandro@datacoves.com",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    description="DAG for testing bash operator.",
    schedule="23 20 * * 1-5",
    tags=["version_1"],
    catchup=False,
)
def dag_test_bash_operator():

    test_bash_operator = BashOperator(
        task_id='hello_world_task',
        bash_command='python -c "print(\'Hello, world!\')"',
        dag=dag
    )

    test_bash_operator

dag = dag_test_bash_operator()
