"""Test DAG: read a variable from Azure Key Vault through the Datacoves
secrets backend chain.

The variable key must start with "datacoves-" so the Datacoves backend
forwards the lookup to the additional (Azure Key Vault) backend. The key
"datacoves-test-secret" maps to the Key Vault secret named
"airflow-variables-datacoves-test-secret".
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
    tags=["azure_key_vault"],
    description="Read a variable from Azure Key Vault",
    schedule=None,
)
def azure_key_vault_test():

    @task
    def read_secret_from_key_vault():
        # Fetch at runtime (inside the task), never at the top level of the
        # DAG file, so Key Vault is only called when the task runs.
        value = Variable.get("datacoves-test-secret")

        # The variable name contains "secret" so Airflow masks the value in
        # logs; compare against the expected value instead of printing it.
        expected = "Datacoves Inc."
        print(f"Fetched a {len(value)} character value from Azure Key Vault")
        if value != expected:
            raise ValueError("Value does not match the secret stored in Key Vault")
        print("SUCCESS: value matches the secret stored in Azure Key Vault")

    read_secret_from_key_vault()


dag = azure_key_vault_test()
