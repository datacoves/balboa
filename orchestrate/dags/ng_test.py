import datetime
from airflow.decorators import dag, task
from airflow.models import Variable


# ❌ BAD PRACTICE: Fetching a variable at the top level
# This will cause Airflow to query for this variable on EVERY DAG PARSE (every 30 seconds),
# which can be costly when using an external secrets manager (e.g., AWS Secrets Manager).
bad_used_variable = Variable.get("bad_used_variable", "default_value")

@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
        "retries": 3
    },
    description="Sample DAG demonstrating bad variable usage",
    schedule="0 0 1 */12 *",
    tags=["extract_and_load","transform"],
    catchup=False,
)
def ng_test():

    # @task.datacoves_dbt(connection_id="main")
    # def show_env_value():
    #     return """
    #         echo dbt_home: && echo $DATACOVES__DBT_HOME &&
    #         echo repo_path: && echo $DATACOVES__REPO_PATH &&
    #         echo cwd: && pwd
    #     """
    # show_env_value()

    @task.datacoves_bash(
        outlets=['Dataset(DatahubPlatform.SNOWFLAKE,  RAW.US_POPULATION.US_POPULATION)'],
        env={
            "UV_CACHE_DIR": "/tmp/uv_cache",
            "EXTRACT__NEXT_ITEM_MODE":"fifo",
            "EXTRACT__MAX_PARALLEL_ITEMS":"1",
            "EXTRACT__WORKERS":"1",
            "NORMALIZE__WORKERS":"1",
            "LOAD__WORKERS":"1",
        },
        append_env=True
    )
    def load_us_population():
        return "cd load/dlt/ && ./us_population.py"

    load_us_population()

# Invoke DAG
dag = ng_test()
