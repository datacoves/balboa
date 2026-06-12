from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils
import logging
import time

logger = logging.getLogger(__name__)


@dag(
    dag_id="test_many_logs",
    catchup=False,
    default_args=datacoves_utils.set_default_args(
        owner="Noel Gomez",
        owner_email="noel@example.com",
    ),
    schedule=datacoves_utils.set_schedule(None),
    description="Sample DAG that prints many logs to verify log streaming",
    tags=["python_script", "sample"],
)
def test_many_logs():

    @task
    def print_logs():
        for i in range(1, 501):
            logger.info(f"Log number {i} - This is a test message to verify log streaming.")
            if i % 50 == 0:
                logger.warning(f"We reached {i} logs")
            time.sleep(0.5)

        logger.info("¡Completed! 500 logs were printed")

    print_logs()


test_many_logs()
