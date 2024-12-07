from airflow.utils.log.logging_mixin import LoggingMixin
from pendulum import datetime, duration
import os

def is_development_environment():
    """Determine if we are running in My Airflow or in Team Airflow"""
    return os.environ.get('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', '').startswith('sqlite')

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
    start_date = datetime(2024, 1, 1),
    retries = 3,
    retry_delay_seconds = None,
    retry_exponential_backoff = False,
    execution_timeout_seconds = None,
    owner = None,
    email = None,
    email_on_failure = True,
    email_on_retry = False,
    on_failure_callback = get_error_handler(),
    **kwargs
):
    """
    Returns default arguments for DAGs with standard error handling and timeouts

    Args:
        start_date (datetime): Start date for the DAG
        retries (int): This ensures that if a task fails, it is retried
        retry_delay_seconds (int): Delay between retries in seconds
        retry_exponential_backoff (bool): Doubles the between the last retry attempt, i.e. Retries after 5, 10, 20 mins
        execution_timeout_seconds (int): Maximum task execution time in seconds
        owner (str): The owner of the DAG (required)
        email (str): Email for notifications (required)
        email_on_failure (bool): Sends email when task fails
        email_on_retry (bool): Sends email when task retries
        on_failure_callback (method): Sets a default method to handle errors
        **kwargs: Additional arguments to override defaults

    Raises:
        ValueError: If owner or email is not provided
    """
    if owner is None:
        raise ValueError("DAG owner must be specified")
    if email is None:
        raise ValueError("Email address must be specified")

    enable_emailing = not is_development_environment()

    default_args = {
        "owner": owner,
        "email": email,
        "start_date": start_date,
        "retries": retries,
        "retry_exponential_backoff": retry_exponential_backoff,
        "email_on_failure": email_on_failure or enable_emailing,
        "email_on_retry": email_on_retry or enable_emailing,
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
    catchup = False,
    max_active_runs = 1,
    max_active_tasks = 10,
    dagrun_timeout_minutes = 5,
    fail_stop = True,
    tags = None,
    owner_links = None,
    **kwargs
):
    """
    Returns default DAG configuration

    Args:
        catchup (bool):  This should always be defined otherwise when the DAG is unpaused Airflow will schedule many runs from the start_date to the current date
        max_active_runs (int): maximum number of concurrent DAG runs. Good to set so multiple runs don't overlap
        max_active_tasks (int):  Max active tasks across all active runs of the DAG
        dagrun_timeout_minutes (int): This will fail the DAG is it takes too long
            In the UI you may see the tasks pass, but the DAG will fail
            downstream tasks will be skipped
            It may be better to set execution_timeout in default parameters
        fail_stop (bool): Fails the whole DAG as soon as a task fails, similar to dbt --fail-fast
            Only works with the "all_success" trigger rule
            https://airflow.apache.org/docs/apache-airflow/1.10.3/concepts.html#trigger-rules

        tags (list): List of tags for the DAG
        owner_links (dict): Dictionary of owner links
        **kwargs: Additional arguments to override defaults
    """
    config = {
        # Used to display the markdown docs at the top of a DAG file in the Airflow UI
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
