from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath

bucket = ObjectStoragePath("s3://datacoves-vs-code-images", conn_id="datacoves_s3")
local_storage = ObjectStoragePath("../../")


@dag(schedule=None, catchup=False, tags=["s3", "version_2"])
def s3_upload_copy():
    @task
    def copy_files():
        local_path = local_storage / ".yamllint"
        destination_path = bucket / ".yamllint"
        local_path.move(destination_path)
        files = [f for f in destination_path.iterdir() if f.is_file()]
        return files

    copy_files()


s3_upload_copy()
