import datetime

from airflow.decorators import dag
from airflow.models import Variable
from operators.datacoves.dbt import DatacovesDbtOperator

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
    run_dbt = DatacovesDbtOperator(
        task_id="run_dbt", bash_command="dbt run -s personal_loans"
    )


dag = bad_variable_usage()
