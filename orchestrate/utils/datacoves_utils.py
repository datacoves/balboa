import os
from datetime import timedelta
from typing import Union
import pendulum

import logging

logger = logging.getLogger(__name__)


############################################
# Constants
############################################

DEV_ENVIRONMENT_SLUG = ""


############################################
# Environment Variables
############################################

def generate_env_exports(env_vars):
    """
    Generates bash export statements for environment variables.

    Args:
        env_vars (dict): Dictionary of environment variables

    Returns:
        str: Bash commands to export all environment variables
    """
    return "; ".join([f'export {k}="{v}"' for k, v in env_vars.items()])


def set_dlt_env_vars(dlt_connections):# = {"sources": {}, "destinations": {}}):
    all_vars = {}
    vars = {}

    if 'sources' in dlt_connections.keys():
        sources = dlt_connections['sources']

        for src in sources:
            prefix = "SOURCE__DATACOVES_SNOWFLAKE__CREDENTIALS__"
            vars.update(connection_to_env_vars(src, prefix, True))

    if 'destinations' in dlt_connections.keys():
        destinations = dlt_connections['destinations']

        for dest in destinations:
            prefix = "DESTINATION__DATACOVES_SNOWFLAKE__CREDENTIALS__"
            vars.update(connection_to_env_vars(dest, prefix, True))

    all_vars = {**vars, **uv_env_vars()}

    return all_vars


def connection_to_env_vars(connection_id, prefix = None, is_for_dlt = False):
    """Environment variables for a Snowflake connection, as Jinja templates that Airflow fills in when the task runs
    (a task's `env` is a templated field).

    Reading the connection here instead would query it on every DAG parse and store the credentials, private key
    included, in the serialized DAG. A missing connection now fails the task when Airflow renders `env`.
    """
    if not prefix:
        prefix = f"DATACOVES__{connection_id.upper()}__"

    conn = f"conn.get({connection_id!r})"
    extra = f"{conn}.extra_dejson"
    private_key = f"({extra}.get('private_key_content') or '')"

    def template(expression):
        return "{{ " + expression + " }}"

    return {
        f"{prefix}{'HOST' if is_for_dlt else 'ACCOUNT'}": template(f"{extra}.get('account') or ''"),
        f"{prefix}{'USERNAME' if is_for_dlt else 'USER'}": template(f"{conn}.login or ''"),
        # The key's body on one line: drop the BEGIN line, the END line and the trailing newline.
        f"{prefix}PRIVATE_KEY": template(f"{private_key}.split('\\n')[1:-2] | join('')"),
        # Empty when there is a private key: Snowflake then uses key-pair auth whatever the password.
        f"{prefix}PASSWORD": template(f"'' if {private_key} else ({conn}.password or '')"),
        f"{prefix}DATABASE": template(f"{extra}.get('database') or ''"),
        f"{prefix}WAREHOUSE": template(f"{extra}.get('warehouse') or ''"),
        f"{prefix}ROLE": template(f"{extra}.get('role') or ''"),
    }


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
    logger = logging.getLogger(__name__)

    # Add this distinctive message
    logger.error("### ENTERING HANDLE_ERROR CALLBACK ###")

    task_instance = context['task_instance']
    task = context['task']
    exception = context.get('exception')

    logger.error(f"""Task Failed!
    Task: {task.task_id}
    Error: {str(exception)}
    Logical Date: {context.get('logical_date')}
    Try Number: {task_instance.try_number}
    """)

############################################
# Default Args
############################################

def set_default_args(owner = None, owner_email = None):
    default_args = {
        # You should ALWAYS define a start time, but this is not when the dag
        # will run, it is a time after which the DAG will run
        "start_date": pendulum.datetime(2025, 1, 1, tz="UTC"),

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
