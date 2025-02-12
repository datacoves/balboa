from airflow.decorators import dag, task
from airflow.models import Variable

@dag(
    default_args={"start_date": "2021-01"},
    description="DAG that echoes S3 password",
    schedule="0 0 1 */12 *",
    tags=["secure"],
    catchup=False,
)
def secure_dag():

    @task.datacoves_bash()
    def transform():
        s3_password = Variable.get("S3_PASSWORD_SECRET")  # ✅ Now retrieved inside the task
        return f"echo 'S3 Password: {s3_password}'"

    transform()

secure_dag()
