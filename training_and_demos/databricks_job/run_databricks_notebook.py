import requests
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

def trigger_notebook(notebook_path, workspace_url, access_token, cluster_id, parameters=None):
    """
    Trigger a Databricks notebook run using the REST API.

    Args:
        notebook_path (str): Full path to the notebook in Databricks workspace
        workspace_url (str): Your Databricks workspace URL (without 'https://')
        access_token (str): Your Databricks personal access token
        cluster_id (str): The ID of the existing Databricks cluster to use
        parameters (dict): Optional parameters to pass to the notebook

    Returns:
        dict: Response from the Databricks API including run_id
    """
    api_endpoint = f"https://{workspace_url}/api/2.0/jobs/runs/submit"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "run_name": f"Triggered Run {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "notebook_task": {
            "notebook_path": notebook_path
        },
        "existing_cluster_id": cluster_id
    }

    # Add parameters if provided
    if parameters:
        payload["notebook_task"]["base_parameters"] = parameters

    try:
        response = requests.post(api_endpoint, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error triggering notebook: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Replace these with your actual values
    WORKSPACE_URL = os.getenv("WORKSPACE_URL")
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    NOTEBOOK_PATH = os.getenv("NOTEBOOK_PATH")
    CLUSTER_ID = os.getenv("CLUSTER_ID")

    # Optional: parameters to pass to the notebook
    params = {
        "param1": "value1",
        "param2": "value2"
    }

    result = trigger_notebook(
        notebook_path = NOTEBOOK_PATH,
        workspace_url = WORKSPACE_URL,
        access_token = ACCESS_TOKEN,
        cluster_id = CLUSTER_ID,
        parameters = params
    )

    if result:
        print(f"Notebook run triggered successfully. Run ID: {result.get('run_id')}")
