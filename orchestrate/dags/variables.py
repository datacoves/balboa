
from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator
from airflow.models import Variable

daily_run_tag = Variable.get("DBT_DAILY_RUN_TAG")

@dag(
    default_args={"start_date": "2021-01"},
    description="Loan Run",
    schedule_interval="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def variables_dag():
    transform = DatacovesDbtOperator(
        task_id="transform",
        bash_command=f"dbt build -s 'tag:{daily_run_tag}'",
    )

dag = variables_dag()
