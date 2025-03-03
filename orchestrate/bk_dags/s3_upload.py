from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath

bucket = ObjectStoragePath("s3://datacoves-vs-code-images", conn_id="datacoves_s3")
local_storage = ObjectStoragePath("/opt/airflow/dags/repo")


@dag(schedule=None, catchup=False, tags=["s3", "version_1"])
def s3_upload_copy():
    @task
    def copy_files():
        local_path = local_storage / "sample.txt"
        destination_path = bucket / "sample.txt"
        local_path.move(destination_path)
        files = [f for f in destination_path.iterdir() if f.is_file()]
        return files

    copy_files()


s3_upload_copy()
