import datetime
import inspect

from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator


@dag(
    default_args={
        "start_date": datetime.datetime(2023, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
    },
    description="Sample DAG for dbt build",
    schedule_interval="0 0 1 */12 *",
    tags=["version_3"],
    catchup=False,
)
def yaml_dbt_dag_mesh():
    run_dbt = DatacovesDbtOperator(
        task_id="run_dbt", bash_command=inspect.cleandoc("""
        echo ###### &&
        export DATACOVES__UPLOAD_MANIFEST_URL=http://gay725-airbyte-airbyte-server-svc/api/internal/manifests
        echo $DATACOVES__UPLOAD_MANIFEST_URL &&
        echo ###### &&
        dbt run -s personal_loans && \
        curl -X POST $DATACOVES__UPLOAD_MANIFEST_URL \
        -H "Authorization: Bearer $DATACOVES__UPLOAD_MANIFEST_TOKEN" \
        -F "environment_slug=$DATACOVES__ENVIRONMENT_SLUG" \
        -F "run_id=$AIRFLOW_CTX_DAG_RUN_ID" \
        -F "file=@target/manifest.json" \
        --max-time 120
        """)
    )

# http://gay725-airbyte-airbyte-server-svc/api/internal/manifests
# http://gay725-airbyte-airbyte-server-svc/api/v2/accounts/1
dag = yaml_dbt_dag_mesh()
