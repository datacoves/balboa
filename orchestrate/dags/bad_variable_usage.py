import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

bad_used_variable = Variable.get("bad_used_variable", "default_value")


@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    description="Sample DAG for dbt build",
    schedule="0 0 1 */12 *",
    tags=["transform"],
    catchup=False,
)
def bad_variable_usage():
    @task
    def extract_and_load_dlt():
        from operators.datacoves.bash import DatacovesBashOperator

        load_us_population = DatacovesBashOperator(
            task_id="load_us_population", bash_command="./load/dlt/load_data.py"
        )

    extract_and_load_dlt()

dag = bad_variable_usage()
