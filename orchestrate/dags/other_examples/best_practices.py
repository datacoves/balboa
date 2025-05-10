"""
## Best Practices Example
This DAG capture several best practices when building DAGs
"""

from pendulum import datetime, duration
import time

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils

# More info on configuration options can be found at
# https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html

# Best Practices Reference
# https://medium.com/indiciumtech/apache-airflow-best-practices-bc0dd0f65e3f
# https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html


# Determine if we are runnig in My Airflow or in Team Airflow
# Normally it is not adviced to have code outside @tasks because
# Airflow will parse Dags every 30 second and slow code can introduce problems
# Don't do this with your code
# This will return True running in My Airflow since that uses sqlite
is_development = (not datacoves_utils.is_team_airflow())

@dag(
    # This is used to display the markdown docs at the top of this file in the Airflow UI when viewing a DAG
    doc_md = __doc__,

    # This should always be defined otherwise when the DAG is unpaused Airflow will schedule many runs from the
    # start_date below to the current date
    catchup = False,

    # This defines when the DAG will run, there are shortcuts like @daily which will schedule the DAG at 12am UTC
    # but it is best to define with a cron expression
    # See this site for a simple way to convert english to cron
    # https://cronprompt.com/
    # i.e. this means "at 3:30am on Jan 22 every year"
    schedule = "30 3 22 1 * *",

    default_args = {
        # You should ALWAYS define a start time, but this is not when the dag
        # will run, it is a time after which the DAG will run
        "start_date": datetime(2024, 1, 1),

        # This ensures that if a task fails, it is retried
        "retries": 3,

        # Wait between retries
        "retry_delay": duration(minutes = 5),

        # Doubles the between the last retry attempt, i.e. Retries after 5, 10, 20 mins
        "retry_exponential_backoff": True,

        # Defines how long a task can take before it is marked as failed
        "execution_timeout": duration(hours = 2),

        # It is a good practice to define the owner of the DAG and enable notifications
        "owner": "Datacoves",
        "email": "noel@example.com",
        "email_on_failure": datacoves_utils.is_team_airflow(),
        "email_on_retry": None,
        "on_failure_callback": datacoves_utils.handle_error,
    },

    # maximum number of concurrent DAG runs
    # Good to set so multiple runs don't overlap
    max_active_runs = 1,

    # Max active tasks across all active runs of the DAG
    max_active_tasks = 10,

    # This will fail the DAG is it takes too long
    # In the UI you may see the tasks pass, but the DAG will fail
    # downstream tasks will be skipped
    # It may be better to set execution_timeout in default parameters
    dagrun_timeout = duration(hours = 6),

    # Fails the whole DAG as soon as a task fails, similar to dbt --fail-fast
    # Only works with the "all_success" trigger rule
    # https://airflow.apache.org/docs/apache-airflow/1.10.3/concepts.html#trigger-rules
    fail_stop = True,

    # Pauses the DAG after x consecutive failed runs
    # This is good to set for a DAG that runs frequently e.g. hourly
    # As you may not want to keep incurring costs unnecesarily
    max_consecutive_failed_dag_runs = 3,

    # Used to group and filter for dags in the UI
    tags = ["sample"],

    # This will make the owner element in the UI open a link
    # vs just filtering all the dags with the specific owner
    # You can have https or mailto: links like mailto:noel@example.com
    owner_links={
        "Datacoves": "https://datacoves.com/product"
    },

    # Name shown in the UI, can include special characters and emojis
    dag_display_name = "Datacoves Best Practices 🚀",

    # Shown when you hover over the DAG name in the UI
    description = "Datacoves DAG to demonstrate best practices",

    # The identifier for the DAG, when not set, the name @dag decorated function name will be used
    dag_id = "dag_best_practices",
)
# Since the dag_id is defined above, we can use a generic name here
def dag_definition():

    @task
    def get_variables():
        from airflow.models import Variable

        sleep_time = Variable.get("sleep_time")
        return int(sleep_time)

    sleep_time = get_variables()

    @task
    def my_task(task_duration):
        print(f"Duration is {task_duration} and type {type(task_duration)}")
        time.sleep(task_duration)
        print("All Done. Task Complete!")

    # Calling Tasks like this sets the dependency implicitly
    my_task(task_duration = sleep_time)

# This invokes the DAG and is always needed
dag_definition()
