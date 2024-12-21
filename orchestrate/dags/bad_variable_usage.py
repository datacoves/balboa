import datetime

from airflow.decorators import dag, task_group
from airflow.models import Variable
from operators.datacoves.bash import DatacovesBashOperator

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
    tags=["extract_and_load"],
    catchup=False,
)
def bad_variable_usage():
    @task_group(group_id="extract_and_load_dlt", tooltip="dlt Extract and Load")
    def extract_and_load_dlt():
        load_us_population = DatacovesBashOperator(
            task_id="load_us_population",
            bash_command="cd load/dlt && ./loans_data.py",
        )

    tg_extract_and_load_dlt = extract_and_load_dlt()


dag = bad_variable_usage()
