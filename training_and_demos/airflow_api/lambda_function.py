import requests
import os
import json

# In Lambda, environment variables are set in the Lambda configuration
# rather than using dotenv
API_URL = os.environ.get("AIRFLOW_API_URL")
API_KEY = os.environ.get("DATACOVES_API_KEY")

def update_dataset(dataset_name):
    url = f"{API_URL}/datasets/events"

    response = requests.post(
        url=url,
        headers={
            "Authorization": f"Token {API_KEY}",
        },
        json={"dataset_uri": dataset_name,}
    )

    try:
        return response.json()
    except ValueError:
        return response.text

def print_response(response):
    if response:
        msg = json.dumps(response, indent=2)
        print(f"Event posted successfully:\n{'='*30}\n\n {msg}")

def lambda_handler(event, context):
    print("Lambda execution started")

    try:
        print(f"Environment variables: API_URL={API_URL is not None}, API_KEY={API_KEY is not None}")

        # Extract S3 information
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        print(f"S3 event details: bucket={bucket}, key={key}")

        print(f"File uploaded: {bucket}/{key}")

        # Airflow Dataset name must be static so if filename changes, that would have to
        # be addressed above
        dataset_name = f"s3://{bucket}/{key}"

        response = update_dataset(dataset_name)
        print_response(response)

        return {
            'statusCode': 200,
            'body': 'Successfully processed S3 event'
        }
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
