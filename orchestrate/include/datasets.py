from airflow.datasets import Dataset

LAMBDA_UPDATED_DATASET = Dataset("s3://my_bucket/my_folder/my_file.csv")
DAG_UPDATED_DATASET = Dataset("upstream_data")
