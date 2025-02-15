from airflow.decorators import dag
from operators.datacoves.dbt import DatacovesDbtOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook
import boto3
import pendulum
import os
from botocore.exceptions import ClientError

@dag(
    doc_md = __doc__,
    default_args = {
        "start_date": pendulum.datetime(2022, 10, 10),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
        "retries": 0,
    },
    catchup = False,
    tags = ["python_script"],
    description = "Datacoves Sample dag",
    schedule = "0 0 1 */12 *",
)
def dbt_run_retry_dag():
    def get_s3_client():
        """Create and return an S3 client using Airflow connection"""
        aws_conn = BaseHook.get_connection("s3_convexa_local")
        return boto3.client(
            's3',
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name=aws_conn.extra_dejson.get('region_name', 'us-east-1')
        )

    def get_s3_path(context, latest=False):
        """Generate S3 path for run_results.json"""
        bucket =  "convexa-local" #Variable.get("S3_BUCKET")
        if latest:
            return f"s3://{bucket}/dbt_runs/latest/run_results.json"
        else:
            dag_run_id = context['dag_run'].run_id
            return f"s3://{bucket}/dbt_runs/{dag_run_id}/run_results.json"

    def download_from_s3(s3_client, s3_path, local_path):
        """Download file from S3 to local path"""
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        try:
            s3_client.download_file(bucket, key, local_path)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"File not found in S3: {s3_path}")
            else:
                print(f"Error downloading from S3: {e}")
            return False

    def upload_to_s3(s3_client, local_path, s3_path):
        """Upload file from local path to S3"""
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        try:
            s3_client.upload_file(local_path, bucket, key)
            return True
        except ClientError as e:
            print(f"Error uploading to S3: {e}")
            return False

    def _check_retry(**context):
        """Check if this is a retry run by looking for run_results.json in S3"""
        target_dir = os.path.join(os.environ.get('DATACOVES__DBT_HOME'), 'target')
        file_path = os.path.join(target_dir, 'run_results.json')
        s3_client = get_s3_client()
        s3_path = get_s3_path(context)

        print("CHECKING IF THIS DAG RAN")
        print(f"Looking for run_results.json at {s3_path}")

        # Create target directory if it doesn't exist
        os.makedirs(target_dir, exist_ok=True)

        # Try to download run_results from S3
        if download_from_s3(s3_client, s3_path, file_path):
            print(f"Downloaded run_results.json from {s3_path}")
            return 'start_retry'
        else:
            print("No previous run_results.json found")
            return 'start_run'

    def handle_run_failure(context):
        """Copy run_results.json to S3 when dbt_run fails"""
        target_dir = os.path.join(os.environ.get('DATACOVES__DBT_HOME'), 'target')
        file_path = os.path.join(target_dir, 'run_results.json')

        if os.path.exists(file_path):
            s3_client = get_s3_client()
            # Upload to run-specific location
            s3_path = get_s3_path(context)
            if upload_to_s3(s3_client, file_path, s3_path):
                print(f"Uploaded run_results.json to {s3_path}")
            else:
                print("Failed to upload run_results.json to S3")

    def handle_run_success(context):
        """Copy successful run_results.json to S3"""
        target_dir = os.path.join(os.environ.get('DATACOVES__DBT_HOME'), 'target')
        file_path = os.path.join(target_dir, 'run_results.json')

        if os.path.exists(file_path):
            s3_client = get_s3_client()
            # Upload to both run-specific and latest locations
            s3_paths = [get_s3_path(context), get_s3_path(context, latest=True)]
            for s3_path in s3_paths:
                if upload_to_s3(s3_client, file_path, s3_path):
                    print(f"Uploaded run_results.json to {s3_path}")
                else:
                    print(f"Failed to upload run_results.json to {s3_path}")

    # Branch operator
    check_retry = BranchPythonOperator(
        task_id='check_retry',
        python_callable=_check_retry
    )

    # Start nodes for each branch
    start_run = DummyOperator(task_id='start_run')
    start_retry = DummyOperator(task_id='start_retry')

    # Main tasks
    dbt_run = DatacovesDbtOperator(
        task_id="dbt_run",
        bash_command="dbt run -s jhu_covid_19+ --profiles-dir ../.vscode",
        on_failure_callback=handle_run_failure,
        on_success_callback=handle_run_success
    )

    dbt_retry = DatacovesDbtOperator(
        task_id="dbt_retry",
        bash_command="dbt retry --profiles-dir ../.vscode",
        on_success_callback=handle_run_success
    )

    final_task = DummyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # Set up the workflow
    check_retry >> [start_run, start_retry]
    start_run >> dbt_run >> final_task
    start_retry >> dbt_retry >> final_task

# Invoke Dag
dag = dbt_run_retry_dag()
