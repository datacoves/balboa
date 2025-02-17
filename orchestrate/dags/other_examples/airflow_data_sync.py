import datetime

from airflow.decorators import dag, task

@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Bruno",
        "email": "bruno@example.com",
        "email_on_failure": False,
        "retries": 3
    },
    description="Sample DAG for dbt build",
    schedule="0 0 1 */12 *",
    tags=["extract_and_load"],
    catchup=False,
)
def airflow_data_sync():
    @task.datacoves_airflow_db_sync(
        db_type="snowflake",
        destination_schema="airflow_dev",
        service_connection_name="main",
        # additional_tables=["additional_table_1", "additional_table_2"],
    )
    def sync_airflow_db():
        pass

    sync_airflow_db()

dag = airflow_data_sync()
