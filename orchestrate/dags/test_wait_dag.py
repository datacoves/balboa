from time import sleep
from airflow.decorators import dag
from airflow.operators.python_operator import PythonOperator

def my_task_function(**kwargs):
    # Add your task logic here
    print("Task is running...")
    sleep(600)  # Sleep for 10 minutes
    print("Task ended after 10min...")


@dag(
    default_args={"start_date": "2021-01"},
    description="Loan Run",
    schedule_interval="0 0 1 */12 *",
    tags=["version_27"],
    catchup=False,
)
def daily_loan_run():
    my_task = PythonOperator(
        task_id='my_task',
        python_callable=my_task_function,
        provide_context=True,
        dag=dag,
    )


dag = daily_loan_run()
