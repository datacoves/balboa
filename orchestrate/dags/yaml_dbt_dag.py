import datetime

from airflow.decorators import dag
from airflow.models import Variable
from operators.datacoves.dbt import DatacovesDbtOperator

print("######33333############")


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    description="Sample DAG for dbt build",
    schedule_interval="0 0 1 */12 *",
    tags=["version_2"],
    catchup=False,
)
def yaml_dbt_dag():
    my_var = Variable.get("mayra_secret")
    other_var = "TESTING"
    run_dbt = DatacovesDbtOperator(
        task_id="run_dbt", bash_command=f"echo TTTTTTTTTTTTT && dbt run -s personal_loans && echo {other_var} && echo TTTTTTTTTTTTT && echo {my_var}"
    )

dag = yaml_dbt_dag()
