"""Test DAG: read a variable through the configured Airflow secrets backend.

Works with any backend selected in Datacoves (Datacoves Secrets Manager,
AWS Secrets Manager, Azure Key Vault, GCP Secret Manager, HashiCorp Vault):
the variable "datacoves-test-secret" must exist in the selected backend
with the value "Datacoves Inc.". Examples:

- Datacoves: a secret named datacoves-test-secret in the Secrets admin,
  shared with the environment and visible to services.
- Azure Key Vault: a secret named airflow-variables-datacoves-test-secret.
- HashiCorp Vault: a KV v2 entry at <mount_point>/<variables_path>/
  datacoves-test-secret with the field "value".
"""

try:
    # Airflow 3
    from airflow.sdk import Variable, dag, task
except ImportError:
    # Airflow 2
    from airflow.decorators import dag, task
    from airflow.models import Variable

from pendulum import datetime


@dag(
    catchup=False,
    default_args={
        "start_date": datetime(2024, 1, 1),
        "owner": "Alejandro Morera",
    },
    tags=["secrets_backend"],
    description="Read a variable through the configured secrets backend",
    schedule=None,
)
def secrets_backend_test():

    @task
    def read_secret_from_backend():
        # Fetch at runtime (inside the task), never at the top level of the
        # DAG file, so the backend is only queried when the task runs.
        value = Variable.get("datacoves-test-secret")

        # The variable name contains "secret" so Airflow masks the value in
        # logs; compare against the expected value instead of printing it.
        expected = "Datacoves Inc."
        print(f"Fetched a {len(value)} character value from the secrets backend")
        if value != expected:
            raise ValueError("Value does not match the expected test secret")
        print("SUCCESS: value matches the secret stored in the backend")
        return "ok"

    read_secret_from_backend()


dag = secrets_backend_test()
