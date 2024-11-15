from airflow.decorators import dag, task
from orchestrate.utils.datacoves import get_default_args, get_default_dag_config
import time

@dag(
    # This defines when the DAG will run, there are shortcuts like @daily which will schedule the DAG at 12am UTC
    # but it is best to define with a cron expression
    # See this site for a simple way to convert english to cron
    # https://cronprompt.com/
    # i.e. this means "at 3:30am on Jan 22 every year"
    schedule = "30 3 22 1 * *",

    default_args = get_default_args(
        owner = "CustomOwner",
        email = "custom@example.com",
        retries = 5  # Override default retries
    ),
    **get_default_dag_config(
        max_active_runs = 2,  # Override default max_active_runs
        tags = ["sample"]  # Override default tags
    ),
    description = "Custom DAG using Datacoves utilities",
    dag_id = "basic_dag",
)
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

    my_task(task_duration = sleep_time)

dag = dag_definition()
