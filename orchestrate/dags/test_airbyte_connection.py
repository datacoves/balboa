"""
## Airbyte connection test
Minimal DAG to verify that the auto-created `airbyte_connection` works:
it triggers a single Airbyte sync and nothing else. Run it manually after
setting the Airflow Variable `airbyte_connection_id` to the connection UUID.
"""

from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from orchestrate.utils import datacoves_utils


@dag(
    doc_md=__doc__,
    catchup=False,
    default_args=datacoves_utils.set_default_args(
        owner="Alejandro Morera",
        owner_email="alejandro@example.com",
    ),
    description="Test DAG for the Airbyte connection",
    schedule=None,
    tags=["test", "extract_and_load"],
)
def test_airbyte_connection():
    AirbyteTriggerSyncOperator(
        task_id="postgres_to_s3",
        # Read at run time from the Airflow Variable, not at parse time.
        connection_id="{{ var.value.airbyte_connection_id }}",
        airbyte_conn_id="airbyte_connection",
    )


test_airbyte_connection()
