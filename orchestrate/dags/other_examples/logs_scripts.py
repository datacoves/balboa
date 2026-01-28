from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import time

logger = logging.getLogger(__name__)


def print_many_logs():
    for i in range(1, 501):
        logger.info(f"Log number {i} - This is a test message to verify log streaming.")
        if i % 50 == 0:
            logger.warning(f"We reached {i} logs")
        time.sleep(0.5)

    logger.info("¡Completed! 500 logs were printed")


with DAG(
    dag_id="test_many_logs",
    start_date=datetime(2025, 1, 27),
    schedule=None,
    catchup=False,
    tags=["test", "logs"],
) as dag:

    task = PythonOperator(
        task_id="print_logs",
        python_callable=print_many_logs,
    )
