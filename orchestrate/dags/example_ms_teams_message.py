from airflow import DAG
from airflow.utils.dates import days_ago
from ms_teams_webhook_operator import MSTeamsWebhookOperator

dag = DAG(
    "Hello_Teams",
    description="Test MSTeams dag",
    schedule_interval="@hourly",
    start_date=days_ago(1),
)

operator = MSTeamsWebhookOperator(
    task_id="msteamtest",
    http_conn_id="airflow-test-webhook",
    message="Hello from Airflow!",
    subtitle="This is the **subtitle**",
    theme_color="00FF00",
    button_text="My button",
    button_url="https://docs.datacoves.com",
    # proxy = "https://yourproxy.domain:3128/",
    dag=dag,
)
