from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 15),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "tags": ["version_1"],
}


@dag(
    default_args=default_args,
    description="DAG globaldevelopment dbt__l1__test",
    schedule=None,
    catchup=False,
)
def dbt__l1__test():
    start_task = DummyOperator(task_id="start")

    @task.datacoves_dbt(connection_id="main_key_pair", run_dbt_deps=True)
    def dbt_test() -> str:
        return "dbt compile"

    dbt_test = dbt_test()

    end_task = DummyOperator(task_id="end")

    start_task >> dbt_test >> end_task


dag_instance = dbt__l1__test()
