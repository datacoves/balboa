from airflow.decorators import dag
from orchestrate.utils.default_args import default_args
from operators.datacoves.dbt import DatacovesDbtOperator


@dag(
    default_args=default_args,
    description="Daily dbt run",
    schedule="0 12 * * *",
    tags=["transform"],
    catchup=False,

)
def default_args_dag():
    run_dbt = DatacovesDbtOperator(
        task_id="run_dbt", bash_command="dbt run -s country_codes"
    )


dag = default_args_dag()
