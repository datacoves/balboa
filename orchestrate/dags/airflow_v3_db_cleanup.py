from pendulum import datetime
from airflow.decorators import dag, task

@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["maintenance"])
def airflow_v3_db_cleanup():
    @task.datacoves_airflow_db_cleanup(retention_days=90, dry_run=True)
    def cleanup():
        pass
    cleanup()

airflow_v3_db_cleanup()
