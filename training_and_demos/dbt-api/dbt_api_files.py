import requests
import os
from dotenv import load_dotenv
import json
from rich import print_json

def str_to_bool(s):
    return s.lower() in ('true', '1', 'yes', 'y')

load_dotenv()
base_url = os.getenv("DATACOVES__API_ENDPOINT")
token = os.getenv("DATACOVES__API_TOKEN")
project_slug = os.getenv("DATACOVES__PROJECT_SLUG")
environment_slug = os.getenv("DATACOVES__ENVIRONMENT_SLUG")

#######################################
# Utility for api interactions
#######################################

def print_format(r):
    print("STATUS:", r.status_code)

    response_text = r.text

    try:
        parsed_json = json.loads(response_text)
        print_json(data=parsed_json)
    except json.JSONDecodeError:
        print("RESPONSE:", response_text)

    print("-----------------------")

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

def get_account(id: int):
    print(f"Getting info for account: {id}")

    r = requests.get(
        url=get_endpoint(endpoint=f"api/v2/accounts/{id}"),
        headers=get_headers(),
    )

    print_format(r)

def get_environments(id: int):
    print(f"Getting environments for account: {id}")

    r = requests.get(
        url=get_endpoint(endpoint=f"api/v2/accounts/{id}/environments"),
        headers=get_headers(),
    )

    print_format(r)

def get_projects(id: int):
    print(f"Getting projects for account: {id}")

    r = requests.get(
        url=get_endpoint(endpoint=f"api/v2/accounts/{id}/projects"),
        headers=get_headers(),
    )

    print_format(r)

def health_check():
    print("Checking Health of api")

    r = requests.get(
        url=get_endpoint(endpoint="/api/v2/healthcheck"),
        headers=get_headers(),
    )

    print_format(r)

# FIXME
def get_files():
    print(f"Getting files for environment: {environment_slug}")

    # r = requests.get(
    #     url=get_endpoint(endpoint=f"/api/v2/datacoves/environments/{environment_slug}/files"),
    #     headers=get_headers(),
    # )

    print_format(r)


#######################################
# Working with files
#######################################

def delete_file(tag: str = ''):

    print(f"Deleting file by tag: {tag}")

    r = requests.delete(
        url=get_endpoint(endpoint=f"/api/v2/datacoves/environments/{environment_slug}/files"),
        headers=get_headers(),
        data={
            "tag": tag
            }
    )

    print_format(r)


def upload_single_file(tag: str):

    print(f"Uploading a single file with tag {tag}")
    files = {"file": open("file1.txt","rb")}
    r = requests.post(
        url=get_endpoint(endpoint=f"/api/v2/datacoves/environments/{environment_slug}/files"),
        headers=get_headers(),
        files=files,
        data={"tag": tag}
    )

    print_format(r)


def upload_multiple_files(file_names: list, tag: str = 'latest'):
    print(f"Uploading files: {file_names}")

    file_root_path = "/config/workspace/transform/target/"
    files = {}
    for index, file in enumerate(file_names):
        files[f"files[{index}][tag]"] = (None, tag)
        files[f"files[{index}][file]"] = (file, open(f"{file_root_path}{file}", "rb"))

    r = requests.post(
        url=get_endpoint(endpoint=f"/api/v2/datacoves/environments/{environment_slug}/files"),
        headers=get_headers(),
        files=files,
    )

    print_format(r)


def get_file_by_name(file_name: str):
    print(f"Getting file {file_name}")
    params = f"filename={file_name}"

    r = requests.get(
        url=get_endpoint(endpoint=f"/api/v2/datacoves/environments/{environment_slug}/files?{params}"),
        headers=get_headers(),
    )

    if r.ok:
        try:
            content = r.json().get("data", {}).get("contents", "")
            if type(content) is dict:
                content = json.dumps(content, indent=4)
        except requests.exceptions.JSONDecodeError:
            content = r.text
        with open(file_name, "w") as f:
            f.write(content)
        print(f"Downloaded {file_name}")
    else:
        print(r.text)


def get_file_by_tag(tag: str):
    print("Getting file by tag: {tag}...")
    params = f"tag={tag}"

    r = requests.get(
        url=get_endpoint(endpoint=f"api/internal/environments/{environment_slug}/files?{params}"),
        headers=get_headers(),
    )

    if r.ok:
        try:
            file_name = r.json().get("data", {}).get("filename", "")
            content = r.json().get("data", {}).get("contents", "")
            if type(content) is dict:
                content = json.dumps(content, indent=4)
        except requests.exceptions.JSONDecodeError:
            content = r.text
        with open(file_name, "w") as f:
            f.write(content)
        print(f"Downloaded {file_name}")
    else:
        print(r.text)


def upload_manifest():
    print("Uploading manifest")
    files = {"file": open("/config/workspace/transform/target/manifest.json", "rb")}

    r = requests.post(
        url=get_endpoint(endpoint="/api/v2/datacoves/manifests"),
        headers=get_headers(),
        files=files,
        data={
            "environment_slug": environment_slug
            }
    )

    print_format(r)


def get_latest_manifest(project_slug: str):
    print("Getting manifest for project: {project_slug}...")

    r = requests.get(
        url=get_endpoint(endpoint=f"/api/v2/datacoves/projects/{project_slug}/latest-manifest"),
        headers=get_headers(),
    )

    file_name = "manifest.json"

    if r.ok:
        try:
            content = r.json().get("data", {}).get("contents", "")
            if type(content) is dict:
                content = json.dumps(content, indent=4)
        except requests.exceptions.JSONDecodeError:
            content = r.text
        with open(file_name, "w") as f:
            f.write(content)
        print(f"Downloaded {file_name}")
    else:
        print(r.text)

if __name__ == "__main__":
    # Get infomration

    # get_account(1)
    # get_environments(1)
    # get_projects(1)
    # health_check()

    # FIXME
    # get_files()

    # Working with files
    # upload_single_file(tag="my-file")
    # delete_file(tag="my-file")

    file_names = ["graph.gpickle", "graph_summary.json", "partial_parse.msgpack"]
    upload_multiple_files(file_names)
    # for file in file_names:
        # delete_file(tag="latest")

    # file_names = ["graph.gpickle", "graph_summary.json", "partial_parse.msgpack"]
    # for file in file_names:
    #     get_file_by_name(file)

    # Will only get one file even if multiple have the same tag
    # get_file_by_tag("latest")

    upload_manifest()
    # get_latest_manifest(project_slug)
