#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "python-dotenv",
#   "requests",
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
from pathlib import Path

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

def get_account(id: int):
    print(f"Getting info for account: {id}")

    r = requests.get(
        url=get_endpoint(endpoint=f"api/v3/accounts/{id}"),
        headers=get_headers(),
    )

    print_responce(r)

def get_projects(account_id: int, project_slug: str = ''):
    print(f"Getting projects for account: {account_id}")

    r = requests.get(
        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}"),
        headers=get_headers(),
    )

    print_responce(r)

def get_environments(account_id: int, project_slug: str, env_slug: str = ''):
    print(f"Getting environments for account: {account_id} and project: {project_slug}")

    print(f"api/v3/accounts/{account_id}projects/{project_slug}/environments")

    r = requests.get(
        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}/environments/{env_slug}"),
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

def list_environment_files(account_id: int, project_slug: str, env_slug: str):
    print(f"Listing files for project: {project_slug} and environment: {env_slug}")

    r = requests.get(
        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}/environments/{env_slug}/files"),
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

def delete_env_file(account_id: int, project_slug: str, env_slug: str,
                    filename: str):

    print(f"Deleting file {filename} from project: {project_slug} in environment: {env_slug}")

    r = requests.delete(
        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}/environments/{env_slug}/files/{filename}"),
        headers=get_headers()
    )

    print_responce(r)

def show_env_file_details(account_id: int, project_slug: str, env_slug: str,
                    filename: str):

    print(f"Showing details for file {filename} in project: {project_slug} in environment: {env_slug}")

    r = requests.get(
        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}/environments/{env_slug}/files/{filename}"),
        headers=get_headers()
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

def download_env_file(account_id: int, project_slug: str, env_slug: str,
                    filename: str):

    print(f"Downloading file {filename} from project: {project_slug} and environment: {env_slug}")

    r = requests.get(
        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}/environments/{env_slug}/files/{filename}/download"),
        headers=get_headers()
    )

    if r.ok:
        try:
            # First we check if we get JSON back
            content = r.json()

            if isinstance(content, dict):
                content = json.dumps(content, indent=4)

            with open(filename, "w") as f:
                f.write(content)
            print(f"Downloaded {filename} successfully")

        except requests.exceptions.JSONDecodeError:
            # Handle binary/non-JSON responses
            content = r.content

            with open(filename, "wb") as f:
                f.write(content)
            print(f"Downloaded {filename} successfully")
    else:
        print(f"Error: {r.status_code}")
        print(r.text)

def download_env_manifest(account_id: int, project_slug: str, env_slug: str, trimmed: bool = False):

    print(f"Downloading manifest from project: {project_slug} and environment: {env_slug}, with trimmed = {trimmed}")

    filename = "manifest.json"

    params = {
        'trim': str(trimmed).lower(),  # Convert to 'true'/'false' string
    }

    r = requests.get(
        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}/environments/{env_slug}/manifest"),
        headers=get_headers(),
        params=params
    )

    if r.ok:
        try:
            # First we check if we get JSON back
            response_data = r.json()
            content = response_data.get("data", {}).get("contents", {})

            if isinstance(content, dict):
                content = json.dumps(content, indent=4)

            with open(filename, "w") as f:
                f.write(content)
            print(f"Downloaded {filename} successfully")

        except requests.exceptions.JSONDecodeError:
            # Handle binary/non-JSON responses
            content = r.content

            print("File does not appear to be a manifest as it was not JSON format")
            print(content)
    else:
        print(f"Error: {r.status_code}")
        print(r.text)

def download_project_manifest(account_id: int, project_slug: str, trimmed: bool = False):

    print(f"Downloading manifest from project: {project_slug} with trimmed = {trimmed}")

    filename = "manifest.json"

    params = {
        'trim': str(trimmed).lower(),  # Convert to 'true'/'false' string
    }

    r = requests.get(
        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}/manifest"),
        headers=get_headers(),
        params=params
    )

    if r.ok:
        try:
            # First we check if we get JSON back
            response_data = r.json()
            content = response_data.get("data", {}).get("contents", {})

            if isinstance(content, dict):
                content = json.dumps(content, indent=4)

            with open(filename, "w") as f:
                f.write(content)
            print(f"Downloaded {filename} successfully")

        except requests.exceptions.JSONDecodeError:
            # Handle binary/non-JSON responses
            content = r.content

            print("File does not appear to be a manifest as it was not JSON format")
            print(content)
    else:
        print(f"Error: {r.status_code}")
        print(r.text)

def download_project_file(account_id: int, project_slug: str, filename: str):

    print(f"Downloading file {filename} from project: {project_slug}")

    r = requests.get(
        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}/files/{filename}/download"),
        headers=get_headers()
    )

    if r.ok:
        try:
            # First we check if we get JSON back
            content = r.json()

            if isinstance(content, dict):
                content = json.dumps(content, indent=4)

            with open(filename, "w") as f:
                f.write(content)
            print(f"Downloaded {filename} successfully")

        except requests.exceptions.JSONDecodeError:
            # Handle binary/non-JSON responses
            content = r.content

            with open(filename, "wb") as f:
                f.write(content)
            print(f"Downloaded {filename} successfully")
    else:
        print(f"Error: {r.status_code}")
        print(r.text)

def delete_project_file(account_id: int, project_slug: str, filename: str):

    print(f"Deleting file {filename} from project: {project_slug}")

    r = requests.delete(
        url=get_endpoint(endpoint=f"api/v3/accounts/{account_id}/projects/{project_slug}/files/{filename}"),
        headers=get_headers()
    )

    print_responce(r)


if __name__ == "__main__":
    # Get infomration

    # health_check()

    # get_account(account_id)

    # get_projects(account_id)
    # get_projects(account_id, project_slug)

    # get_environments(account_id, project_slug)
    # get_environments(account_id, project_slug, environment_slug)

    # Work with files
    cols = ["environment_slug",'filename', 'metadata', 'inserted_at']

    # project_files = list_project_files(account_id, project_slug)
    # print_table(project_files, cols)

    # environment_files = list_environment_files(account_id, project_slug, environment_slug)
    # print_table(environment_files, cols)

    filenames = ["graph.gpickle", "graph_summary.json", "partial_parse.msgpack"]

    # UPLOAD FILES
    # for filename in filenames:
    #     upload_env_file(account_id, project_slug, environment_slug, filename)

    # upload_env_file(account_id, project_slug, environment_slug, "manifest.json", is_manifest=True )

    # DELETE FILES
    # for filename in filenames:
    #     delete_env_file(account_id, project_slug, environment_slug, filename)

    # delete_env_file(account_id, project_slug, environment_slug, "manifest.json")

    # SHOW FILE DETAILS
    # for filename in filenames:
    #     show_env_file_details(account_id, project_slug, environment_slug, filename)

    # DOWNLOAD Files
    # for filename in filenames:
    #     download_env_file(account_id, project_slug, environment_slug, filename)

    # download_env_manifest(account_id, project_slug, environment_slug, trimmed = True)

    # for filename in filenames:
    #     promote_env_file(account_id, project_slug, environment_slug, filename)
    #     download_project_file(account_id, project_slug, filename)

    # DELETE FILES
    # for filename in filenames:
    #     delete_project_file(account_id, project_slug, filename)

    # promote_env_file(account_id, project_slug, environment_slug, "manifest.json" )
    # download_project_manifest(account_id, project_slug, trimmed = True)

    files = list_project_files(account_id, project_slug)
    print_table(files, cols)
