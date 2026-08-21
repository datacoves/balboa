"""Test DAG: send an email using the smtp_default connection.

Datacoves creates the smtp_default connection through the Airflow REST API
(independent of the secrets backend in use); host and port come from the
AIRFLOW__SMTP__* settings derived from the environment's SMTP integration.
On the local cluster the mail is captured by Mailpit
(https://mailpit.datacoveslocal.com), so any recipient address works.
"""

try:
    # Airflow 3 (smtp provider)
    from airflow.providers.smtp.operators.smtp import EmailOperator
except ImportError:
    # Airflow 2
    from airflow.operators.email import EmailOperator

try:
    # Airflow 3
    from airflow.sdk import dag
except ImportError:
    # Airflow 2
    from airflow.decorators import dag

from pendulum import datetime


@dag(
    catchup=False,
    default_args={
        "start_date": datetime(2024, 1, 1),
        "owner": "Alejandro Morera",
    },
    tags=["smtp"],
    description="Send a test email through smtp_default",
    schedule=None,
)
def smtp_test():

    EmailOperator(
        task_id="send_test_email",
        to="test@datacoveslocal.com",
        subject="SMTP test from Airflow",
        html_content="<b>It works!</b> Sent through the smtp_default connection.",
    )


dag = smtp_test()
