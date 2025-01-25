import datetime

from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator


@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
        "retries": 3,
    },
    description="Sample DAG for dbt build",
    schedule="0 0 1 */12 *",
    tags=["transform"],
    catchup=False,
)
def yaml_dbt_dag():
    # my_var = Variable.get("ngtest")
    # my_aws_var = Variable.get("aws_ngtest")
    # my_aws_secret = Variable.get('aws_ngtest_password')
    # datacoves_secret = Variable.get('mayras_secret')
    # if my_var == 'noel':
    #     other_var = "READIT"
    # else:
    #     other_var = "NOSECRET"

    run_dbt = DatacovesDbtOperator(
        task_id="run_dbt",
        bash_command="dbt run --warn-error -s personal_loans_fail",
        # bash_command=f"echo TTTTTTTTTTTTT && dbt run -s personal_loans && echo && echo TTTTTTTTTTTTT && echo {my_var}"
        # bash_command=f"echo TTTTTTTTTTTTT && dbt run -s personal_loans && echo && echo TTTTTTTTTTTTT && echo {my_var} && echo {my_aws_var} && echo {my_aws_secret}"
        # bash_command=f"echo TTTTTTTTTTTTT && dbt run -s personal_loans && echo && echo TTTTTTTTTTTTT && echo {my_var} && echo {my_aws_var} && echo {my_aws_secret} && echo {mayras_secret}"

    )



dag = yaml_dbt_dag()
