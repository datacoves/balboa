import requests
import os
from dotenv import load_dotenv


load_dotenv()
base_url_internal = os.getenv("DATACOVES__UPLOAD_MANIFEST_URL")
base_url_external = os.getenv("DATACOVES__EXTERNAL_URL")
sa_user = os.getenv("DATACOVES__SECRETS_TOKEN")
sa_airflow = os.getenv("DATACOVES__UPLOAD_MANIFEST_TOKEN")
internal_bearer_token =  os.getenv("DATACOVES__INTERNAL_BEARER_TOKEN")
environment_slug = os.getenv("DATACOVES__ENVIRONMENT_SLUG")


def print_format(r):
    print("STATUS:", r.status_code)
    if r.text:
        print("RESPONSE:", r.text)
    print("-----------------------")


def get_internal_endpoint(endpoint: str) -> str:
    return f"{base_url_internal}/{endpoint}"


def get_headers(token: str) -> dict:
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

def delete_file(tag: str):
    print(f"Deleting file by tag: {tag}...")
    r = requests.delete(
        url=get_internal_endpoint(endpoint=f"api/internal/environments/{environment_slug}/files"),
        headers=get_headers(token=sa_airflow),
        data={ "tag": tag}
    )

    print_format(r)

def upload_single_file():
    print("Uploading a single file...")
    files = {"file": open("file1.txt","rb")}
    r = requests.post(
        url=get_internal_endpoint(endpoint=f"api/internal/environments/{environment_slug}/files"),
        headers=get_headers(token=sa_airflow),
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
        url=get_internal_endpoint(endpoint=f"api/internal/environments/{environment_slug}/files"),
        headers=get_headers(token=sa_airflow),
        files=files,
    )

    print_format(r)


def update_file_by_tag(tag: str):
    print("Updating file by tag: {tag}...")
    files = {"file": open("file2.txt", "rb")}

    r = requests.put(
        url=get_internal_endpoint(endpoint=f"api/internal/environments/{environment_slug}/files"),
        headers=get_headers(token=sa_airflow),
        files=files,
        data={ "tag": "some-tag"}
    )

    print_format(r)


def get_file_by_tag(tag: str):
    print("Getting file by tag: {tag}...")
    params = f"tag={tag}"

    r = requests.get(
        url=get_internal_endpoint(endpoint=f"api/internal/environments/{environment_slug}/files?{params}"),
        headers=get_headers(token=sa_airflow),
    )

    print_format(r)

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
    get_file_by_tag("some-tag")
