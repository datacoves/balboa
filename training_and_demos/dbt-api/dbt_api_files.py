import requests
import os
from dotenv import load_dotenv
import json

def str_to_bool(s):
    return s.lower() in ('true', '1', 'yes', 'y')

load_dotenv()
base_url = os.getenv("DATACOVES__DBT_API_INTERNAL_URL")
token = os.getenv("DATACOVES__DBT_API_TOKEN")
environment_slug = os.getenv("DATACOVES__ENVIRONMENT_SLUG")

def print_format(r):
    print("STATUS:", r.status_code)
    if r.text:
        print("RESPONSE:", r.text)
    print("-----------------------")

def get_endpoint(endpoint: str) -> str:
    return f"{base_url}/{endpoint}"


def get_headers() -> dict:
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

def delete_file(tag: str):
    print(f"Deleting file by tag: {tag}...")
    r = requests.delete(
        url=get_endpoint(endpoint=f"api/internal/environments/{environment_slug}/files"),
        headers=get_headers(),
        data={ "tag": tag}
    )

    print_format(r)

def upload_single_file():
    print("Uploading a single file...")
    files = {"file": open("file1.txt","rb")}
    r = requests.post(
        url=get_endpoint(endpoint=f"api/internal/environments/{environment_slug}/files"),
        headers=get_headers(),
        files=files,
        data={"tag": "some-tag"}
    )

    print_format(r)


def upload_multiple_files():
    print("Uploading multiple files...")
    file1_path = "file1.txt"
    file2_path = "file2.txt"

    files = {
        "files[0][tag]": (None, "tag-1"),
        "files[0][file]": ("some_filename_1.txt", open(file1_path, "rb")),
        "files[1][tag]": (None, "tag-2"),
        "files[1][file]": ("some_filename_2.txt", open(file2_path, "rb")),
    }

    r = requests.post(
        url=get_endpoint(endpoint=f"api/internal/environments/{environment_slug}/files"),
        headers=get_headers(),
        files=files,
    )

    print_format(r)


def update_file_by_tag(tag: str):
    print("Updating file by tag: {tag}...")
    files = {"file": open("file2.txt", "rb")}

    r = requests.put(
        url=get_endpoint(endpoint=f"api/internal/environments/{environment_slug}/files"),
        headers=get_headers(),
        files=files,
        data={ "tag": "some-tag"}
    )

    print_format(r)


def get_file_by_tag(tag: str):
    print("Getting file by tag: {tag}...")
    params = f"tag={tag}"

    r = requests.get(
        url=get_endpoint(endpoint=f"api/internal/environments/{environment_slug}/files?{params}"),
        headers=get_headers(),
    )

    print_format(r)



def update_dbt_static_files():
    print("Updating static dbt files...")

    files = {
        "files[0][file]": ("manifest.json", open("/config/workspace/transform/target/manifest.json", "rb")),
        "files[0][tag]": (None, "latest"),
        "files[1][file]": ("partial_parse.msgpack", open("/config/workspace/transform/target/partial_parse.msgpack", "rb")),
        "files[1][tag]": (None, "latest"),
        "files[2][file]": ("graph_summary.json", open("/config/workspace/transform/target/graph_summary.json", "rb")),
        "files[2][tag]": (None, "latest"),
        "files[3][file]": ("graph.gpickle", open("/config/workspace/transform/target/graph.gpickle", "rb")),
        "files[3][tag]": (None, "latest"),
    }
    print("test")
    r = requests.post(
        url=get_endpoint(endpoint=f"api/internal/environments/{environment_slug}/files"),
        headers=get_headers(),
        files=files,
    )
    if r.ok:
        print("Done uploading files")
    else:
        print(r.text)

def get_static_files(file_name: str):
    print(f"Getting file {file_name}...")
    params = f"filename={file_name}"

    r = requests.get(
        url=get_endpoint(endpoint=f"api/internal/environments/{environment_slug}/files?{params}"),
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


if __name__ == "__main__":
    # Upload single file
    # delete_file(tag="some-tag")
    # upload_single_file()

    # Upload multiple files
    # delete_file(tag="tag-1")
    # delete_file(tag="tag-2")
    # upload_multiple_files()

    # Update file
    # delete_file(tag="some-tag")
    # upload_single_file()
    # update_file_by_tag("some-tag")

    # Get file
    # get_file_by_tag("some-tag")

    # update_dbt_static_files()

    get_static_files("manifest.json")
    # get_static_files("partial_parse.msgpack")
    # get_static_files("graph_summary.json")
    # get_static_files("graph.gpickle")
