import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()
API_URL =  os.getenv("AIRFLOW_API_URL")
API_KEY =  os.getenv("DATACOVES_API_KEY")

def update_dataset(name):
    url=f"{API_URL}/datasets/events"

    response = requests.post(
        url=url,
        headers = {
            "Authorization": f"Token {API_KEY}",
        },
        json={"dataset_uri": "upstream_data",}
    )

    try:
        return response.json()
    except ValueError:
        return response.text


def trigger_dag(dag_id):
    url=f"{API_URL}/dags/{dag_id}/dagRuns"

    response = requests.post(
        url=url,
        headers = {
            "Authorization": f"Token {API_KEY}",
        },
        json={"note": "Trigger from API",}
    )
    return response.json()


def list_dags():
    url=f"{API_URL}/dags"

    response = requests.get(
        url=url,
        headers = {
            "Authorization": f"Token {API_KEY}",
             "Content-Type": "application/json",
        },
    )

    # Extract just the DAG names from the response
    dags_data = response.json()
    dag_names = [dag['dag_id'] for dag in dags_data['dags']]

    # Sort the names alphabetically for better readability
    dag_names.sort()
    return dag_names


def print_response(response):
    if response:
        msg = json.dumps(response, indent=2)
        print(f"Event posted successfully:\n{'='*30}\n\n {msg}")


if __name__ == "__main__":

    # Update an Airflow Dataset
    dataset_name = "upstream_data"
    response = update_dataset(dataset_name)
    print_response(response)

    # Trigger a DAG
    # dag_id = "bad_variable_usage"
    # response = trigger_dag(dag_id)
    # print_response(response)

    # List DAGs
    # response = list_dags()
    # print_response(response)
