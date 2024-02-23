from airflow.decorators import dag
from operators.datacoves.bash import DatacovesBashOperator
from airflow.models import Connection

airbyte_connection = Connection.get(conn_id="AIRBYTE_CONNECTION")

@dag(
    default_args={"start_date": "2021-01"},
    description="DAG that outputs Airbyte Hostname",
    schedule_interval="0 0 1 */12 *",
    tags=["version_1"],
    catchup=False,
)
def connections_dag():
    echo_airbyte_host = DatacovesBashOperator(
        task_id="echo_airbyte_host",
        bash_command=f"echo 'Airbyte hostname is {airbyte_connection.host}'",
    )

dag = connections_dag()
