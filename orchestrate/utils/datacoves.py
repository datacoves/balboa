from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.hooks.base import BaseHook
from pendulum import datetime, duration
import os

def last_dag_successful_run(dag_id):
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


def connection_to_env_vars(connection_id):
    # Get the connection object
    conn = BaseHook.get_connection(connection_id)

    vars = {}
    prefix = f"DATACOVES__{connection_id.upper()}__"

    # Access specific connection parameters
    vars[f"{prefix}ACCOUNT"] = conn.extra_dejson.get('account')
    vars[f"{prefix}DATABASE"] = conn.extra_dejson.get('database')
    vars[f"{prefix}WAREHOUSE"] = conn.extra_dejson.get('warehouse')
    vars[f"{prefix}ROLE"] = conn.extra_dejson.get('role')
    vars[f"{prefix}USER"] = conn.login
    vars[f"{prefix}PASSWORD"] = conn.password

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


def is_not_my_airflow():
    """Returns True if running DAG in not in My Airflow"""
    return os.getenv("DATACOVES__AIRFLOW_TYPE") != "my_airflow"


def get_error_handler():
    """Returns the standard error handling callback function"""
    def handle_error(context):
        """Callback function to handle task failures"""
        logger = LoggingMixin().log
        task_instance = context['task_instance']
        task = context['task']
        exception = context.get('exception')

        logger.error(f"""Task Failed!
        Task: {task.task_id}
        Error: {str(exception)}
        Execution Date: {context['execution_date']}
        Try Number: {task_instance.try_number}
        """)

    return handle_error


def get_default_args(
    start_date=datetime(2024, 1, 1),
    retries=3,
    retry_delay_seconds=None,
    retry_exponential_backoff=False,
    execution_timeout_seconds=None,
    owner=None,
    email=None,
    email_on_failure=None,
    email_on_retry=False,
    on_failure_callback=None,
    **kwargs
):
    """
    Returns default arguments for DAGs with standard error handling and timeouts.

    Args:
        start_date (datetime): Start date for the DAG.
        retries (int): Number of retries on task failure.
        retry_delay_seconds (int): Delay between retries in seconds.
        retry_exponential_backoff (bool): Exponentially increases retry delay.
        execution_timeout_seconds (int): Maximum task execution time in seconds.
        owner (str): The owner of the DAG (required).
        email (str): Email for notifications (required).
        email_on_failure (bool): Sends email when task fails.
        email_on_retry (bool): Sends email when task retries.
        on_failure_callback (method): Error handling method.
        **kwargs: Additional arguments to override defaults.

    Raises:
        ValueError: If owner or email is not provided.
    """
    if owner is None:
        raise ValueError("DAG owner must be specified")
    if email is None:
        raise ValueError("Email address must be specified")

    enable_emailing = is_not_my_airflow()

    default_args = {
        "owner": owner,
        "email": email,
        "start_date": start_date,
        "retries": retries,
        "retry_exponential_backoff": retry_exponential_backoff,
        "email_on_failure": enable_emailing,
        "email_on_retry": email_on_retry,
        "on_failure_callback": on_failure_callback,
    }

    if retry_delay_seconds is not None:
        default_args["retry_delay"] = duration(minutes=retry_delay_seconds)

    if execution_timeout_seconds is not None:
        default_args["execution_timeout"] = duration(minutes=execution_timeout_seconds)

    # Override or add any additional arguments
    default_args.update(kwargs)
    return default_args

def get_default_dag_config(
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    dagrun_timeout_minutes=5,
    fail_stop=True,
    tags=None,
    owner_links=None,
    **kwargs
):
    """
    Returns default DAG configuration.

    Args:
        catchup (bool): Ensures Airflow doesn’t backfill unscheduled DAG runs.
        max_active_runs (int): Maximum concurrent DAG runs.
        max_active_tasks (int): Maximum concurrent tasks across DAG runs.
        dagrun_timeout_minutes (int): Auto-fails DAG if execution time exceeds threshold.
        fail_stop (bool): Stops all downstream tasks if a task fails.
        tags (list): List of tags for the DAG.
        owner_links (dict): Dictionary of owner links.
        **kwargs: Additional arguments to override defaults.
    """
    config = {
        "doc_md": __doc__,
        "catchup": catchup,
        "max_active_runs": max_active_runs,
        "max_active_tasks": max_active_tasks,
        "fail_stop": fail_stop,
        "tags": tags,
        "owner_links": owner_links,
    }

    # Only add dagrun_timeout if minutes is specified
    if dagrun_timeout_minutes is not None:
        config["dagrun_timeout"] = duration(minutes=dagrun_timeout_minutes)


    # Override or add any additional arguments
    config.update(kwargs)
    return config
