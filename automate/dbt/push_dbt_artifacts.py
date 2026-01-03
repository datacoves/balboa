#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "requests",
#   "python-dotenv",
#   "rich",
# ]
# ///

import requests
import os
from dotenv import load_dotenv
import json
from rich import print_json
from rich.console import Console
from rich.table import Table


load_dotenv()
base_url = os.getenv("DATACOVES__API_ENDPOINT")
token = os.getenv("DATACOVES__API_TOKEN")
account_id = os.getenv("DATACOVES__ACCOUNT_ID")
project_slug = os.getenv("DATACOVES__PROJECT_SLUG")
environment_slug = os.getenv("DATACOVES__ENVIRONMENT_SLUG")
dbt_home = os.getenv("DATACOVES__DBT_HOME")


#######################################
# Utility for api interactions
#######################################
def print_responce(r):
    print("STATUS:", r.status_code)

    response_text = r.text

    try:
        parsed_json = json.loads(response_text)
        print_json(data=parsed_json)
    except json.JSONDecodeError:
        print("RESPONSE:", response_text)

    print("-----------------------")

def print_table(items, keys_to_show, title="Items"):
    """Print a table showing only specified keys from a list of dictionaries"""
    console = Console()
    table = Table(title=title)

    # Define different colors for each column
    colors = ["blue", "bright_green", "yellow", "green", "cyan", "magenta", "red", "bright_cyan", "bright_magenta", "bright_yellow"]

    # Add columns for each key we want to show with different colors
    for index, key in enumerate(keys_to_show):
        color = colors[index % len(colors)]  # Cycle through colors if more columns than colors
        table.add_column(key.replace('_', ' ').title(), style=color)

    # Add rows for each item in the list
    for item in items:
        row_values = []
        for key in keys_to_show:
            value = item.get(key, "N/A")
            row_values.append(str(value))
        table.add_row(*row_values)

    console.print(table)

def get_endpoint(endpoint: str) -> str:
    return f"{base_url}/{endpoint}"

def get_headers() -> dict:
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

#######################################
# Get information
#######################################

def health_check():
    print("Checking Health of api")

    r = requests.get(
        url=get_endpoint(endpoint="/api/v3/healthcheck"),
        headers=get_headers(),
    )

    print_responce(r)

#######################################
# Working with files
#######################################

def list_project_files(account_id: int, project_slug: str):
    print(f"Listing files for project: {project_slug}")

    r = requests.get(
        # url=get_endpoint(endpoint=f"/api/v3/datacoves/account/{account_id}/projects/{project_slug}/files"),

        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}/files"),

        headers=get_headers(),
    )

    return r.json().get("data", {})

def upload_env_file(account_id: int, project_slug: str, env_slug: str,
                    filename: str, is_manifest: bool = False,
                    dag_id: str = None, run_id: str = None, use_multipart: bool = False):

    print(f"Uploading file {filename} to project: {project_slug} in environment: {env_slug}")

    file = {"file": (filename, open(f"{dbt_home}/target/{filename}", "rb"))}

    data = {
        'filename': filename,
        'is_manifest': str(is_manifest).lower()
    }

    r = requests.post(
        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}/environments/{env_slug}/files"),
        headers=get_headers(),
        files=file,
        data=data
    )

    print_responce(r)

def promote_env_file(account_id: int, project_slug: str, env_slug: str,
                    filename: str):

    print(f"Promoting file {filename} in environment: {env_slug} to project level ({project_slug})")

    r = requests.post(
        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}/environments/{env_slug}/files/{filename}/promote"),
        headers=get_headers()
    )

    print_responce(r)

def delete_project_file(account_id: int, project_slug: str, filename: str):

    print(f"Deleting file {filename} from project: {project_slug}")

    r = requests.delete(
        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}/files/{filename}"),
        headers=get_headers()
    )

    print_responce(r)

if __name__ == "__main__":
    # Get infomration

    health_check()

    cols = ["environment_slug",'filename', 'metadata', 'inserted_at']

    # UPLOAD FILES

    filenames = ["graph.gpickle", "graph_summary.json", "partial_parse.msgpack"]
    for filename in filenames:
        upload_env_file(account_id, project_slug, environment_slug, filename)

    for filename in filenames:
        promote_env_file(account_id, project_slug, environment_slug, filename)

    upload_env_file(account_id, project_slug, environment_slug, "manifest.json", is_manifest=True )
    promote_env_file(account_id, project_slug, environment_slug, "manifest.json" )

    # delete_project_file(account_id, project_slug, "manifest.json")

    # SHOW FILE DETAILS
    files = list_project_files(account_id, project_slug)
    print_table(files, cols)
