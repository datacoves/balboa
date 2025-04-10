from airflow.datasets import Dataset

# A dataset can be anything, it will be a poiner in the Airflow db.
# If you need to access url like s3://my_bucket/my_file.txt then you can set
# it with the proper path for reuse.

LAMBDA_UPDATED_DATASET = Dataset("s3://my_bucket/my_folder/my_file.csv")
DAG_UPDATED_DATASET = Dataset("upstream_data")
