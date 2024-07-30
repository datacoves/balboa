"""## Datacoves Bash Operator DAG
This DAG is a sample using the Datacoves Airflow Operators"""

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from pendulum import datetime


@dag(
    default_args={
        "start_date": datetime(2022, 10, 10),
        "owner": "Bruno Antonellini",
        "email": "bruno@datacoves.com",
        "email_on_failure": True,
    },
    catchup=False,
    tags=["version_1"],
    description="Fail and send email",
    # This is a regular CRON schedule. Helpful resources
    # https://cron-ai.vercel.app/
    # https://crontab.guru/
    schedule_interval="0 0 1 */12 *",
)
def fail_and_send_email():
    @task(email=["bruno@datacoves.com"], email_on_failure=True, email_on_retry=True)
    def t1():
        raise AirflowException("This is a test exception")


# Invoke Dag
dag = fail_and_send_email()
dag.doc_md = __doc__
