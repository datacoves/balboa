from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator
from pendulum import datetime


@dag(
    default_args={
        "start_date": datetime(2022, 10, 10),
        "owner": "Bruno Antonellini",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    catchup=False,
    tags=["version_1"],
    description="Datacoves Secrets Backend dag",
    # This is a regular CRON schedule. Helpful resources
    # https://cron-ai.vercel.app/
    # https://crontab.guru/
    schedule_interval="0 0 1 */12 *",
)
def datacoves_secrets_backend():
    # Calling dbt commands
    echo_simple_secret = DatacovesBashOperator(
        task_id="echo_simple_secret",
        bash_command="echo ${snowflake_password}",
        env={"snowflake_password": "{{ var.value.get('snowflake_password') }}"},
    )

    # This is calling an external Python file after activating the venv
    # use this instead of the Python Operator
    echo_complex_secret = DatacovesBashOperator(
        task_id="echo_complex_secret",
        # Virtual Environment is automatically activated
        # activate_venv=True,
        bash_command="echo ${all_paswords}",
        env={"all_paswords": "{{ var.value.get('all_paswords') }}"},
    )

    # Define task dependencies
    echo_complex_secret.set_upstream([echo_simple_secret])


# Invoke Dag
dag = datacoves_secrets_backend()
