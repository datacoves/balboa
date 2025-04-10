import os
from datetime import datetime, timedelta
from typing import Union

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException

############################################
# Constants
############################################

DEV_ENVIRONMENT_SLUG = "dev123"


############################################
# Environment Variables
############################################

def connection_to_env_vars(connection_id):
    vars = {}

    try:
        # Get the connection object
        conn = BaseHook.get_connection(connection_id)

        prefix = f"DATACOVES__{connection_id.upper()}__"

        # Access specific connection parameters
        vars[f"{prefix}ACCOUNT"] = conn.extra_dejson.get('account')
        vars[f"{prefix}DATABASE"] = conn.extra_dejson.get('database')
        vars[f"{prefix}WAREHOUSE"] = conn.extra_dejson.get('warehouse')
        vars[f"{prefix}ROLE"] = conn.extra_dejson.get('role')
        vars[f"{prefix}USER"] = conn.login
        vars[f"{prefix}PASSWORD"] = conn.password

    except AirflowNotFoundException:
        pass

    return vars


def uv_env_vars():
    uv_vars = {
        "UV_CACHE_DIR": "/tmp/uv_cache",
        "EXTRACT__NEXT_ITEM_MODE": "fifo",
        "EXTRACT__MAX_PARALLEL_ITEMS": "1",
        "EXTRACT__WORKERS": "1",
        "NORMALIZE__WORKERS": "1",
        "LOAD__WORKERS": "1",
    }

    return uv_vars

############################################
# DAG utilities
############################################

def is_team_airflow():
    """Returns True if running DAG in not in My Airflow"""
    return os.getenv("DATACOVES__AIRFLOW_TYPE", "") == "team_airflow"

def get_last_dag_successful_run_date(dag_id):
    from airflow.models.dagrun import DagRun
    from airflow.utils.state import DagRunState

    last_successful_run = DagRun.find(
        dag_id = dag_id,
        state = DagRunState.SUCCESS,
    )
    if last_successful_run:
        last_successful_run.sort(key=lambda x: x.execution_date, reverse=True)
        last_run = last_successful_run[0]
        print(f"Last successful run of DAG {dag_id}: {last_run.execution_date}")
        return last_run.execution_date
    else:
        print(f"No previous successful runs found for DAG {dag_id}")
        return None

# Sets Schedule to None in My Airflow and in Development Environment based on Datacoves Environment Slug defined above
def set_schedule(default_input: Union[str, None]) -> Union[str, None]:
    """
    Sets the application's schedule based on the current environment setting. Allows you to
    set the the default for dev to none and the the default for prod to the default input.

    This function checks the Datacoves Slug through 'DATACOVES__ENVIRONMENT_SLUG' variable to determine
    if the application is running in a specific environment (e.g., 'dev123'). If the application
    is running in the 'dev123' environment, it indicates that no schedule should be used, and
    hence returns None. For all other environments, the function returns the given 'default_input'
    as the schedule.

   Parameters:
    - default_input (Union[str, None]): The default schedule to return if the application is not
      running in the dev environment.

    Returns:
    - Union[str, None]: The default schedule if the environment is not 'dev123'; otherwise, None,
      indicating that no schedule should be used in the dev environment.
    """
    env_slug = os.environ.get("DATACOVES__ENVIRONMENT_SLUG", "").lower()
    if (env_slug == DEV_ENVIRONMENT_SLUG) or (not is_team_airflow()):
        return None
    else:
        return default_input


############################################
# Error Handler
############################################

def handle_error(context):
    """Callback function to handle task failures"""
    logger = LoggingMixin().log

    # Add this distinctive message
    logger.error("### ENTERING HANDLE_ERROR CALLBACK ###")

    task_instance = context['task_instance']
    task = context['task']
    exception = context.get('exception')

    logger.error(f"""Task Failed!
    Task: {task.task_id}
    Error: {str(exception)}
    Execution Date: {context['execution_date']}
    Try Number: {task_instance.try_number}
    """)

############################################
# Default Args
############################################

def set_default_args(owner = None, owner_email = None):
    default_args = {
        # You should ALWAYS define a start time, but this is not when the dag
        # will run, it is a time after which the DAG will run
        "start_date": datetime(2025, 1, 1),

        # This ensures that if a task fails, it is retried
        "retries": 3,

        # Wait between retries
        'retry_delay': timedelta(minutes = 5),

        # Doubles the between the last retry attempt, i.e. Retries after 5, 10, 20 mins
        "retry_exponential_backoff": True,

        # Defines how long a task can take before it is marked as failed
        'execution_timeout': timedelta(hours = 2),

        # It is a good practice to define the owner of the DAG and enable notifications
        "owner": owner,
        "email": owner_email,
        "email_on_failure": is_team_airflow(),
        "email_on_retry": None,
        "on_failure_callback": handle_error,
    }

    return default_args
