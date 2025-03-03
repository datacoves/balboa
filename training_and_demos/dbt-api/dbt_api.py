import os
import requests
from importlib import reload
from http import HTTPStatus
from dotenv import load_dotenv

import json
from pygments import highlight, lexers, formatters

def get_headers(token: str) -> dict:
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }
    # print("HEADERS:", headers)
    return headers

def get_internal_endpoint(endpoint: str) -> str:
    url = f"{base_url_internal}/{endpoint}"
    print("URL:", url, "\n")
    return url

def get_external_endpoint(endpoint: str) -> str:
    url = f"{base_url_external}/{endpoint}"
    print("URL:", url, "\n")
    return url

def check_health():
    r = requests.get(url=get_internal_endpoint(endpoint="api/internal/healthcheck"))
    print_format(r)

def gen_public_token():
    payload = {"token": {"account_id": 1}}
    r = requests.post(
        url=get_internal_endpoint(endpoint="api/internal/tokens"),
        headers={"Authorization": f"Bearer {internal_bearer_token}"},
        json=payload
    )
    # print_format(r)
    token = r.json()["data"]["token"]
    return token

def delete_public_token():
    account_id = 1
    r = requests.delete(
        url=get_internal_endpoint(endpoint=f"api/internal/tokens/{account_id}"),
        headers={"Authorization": f"Bearer {internal_bearer_token}"},
    )
    # print_format(r)
    return r.status_code

def regen_public_token():
    if delete_public_token() == HTTPStatus.NO_CONTENT:
        token = gen_public_token()
    else:
        token = "PUBLIC TOKEN: Could not be created!!!"
    return token

def upload_manifest():
    files = {'file': open('/config/workspace/transform/target/manifest.json','rb')}
    url =get_internal_endpoint(endpoint="api/internal/manifests")
    headers = get_headers(token=sa_airflow)
    data={
            "environment_slug": environment_slug,
            "run_id": "manual__2025-03-03T13:24:16.578406+00:00", # TODO tiene que ser un run_id valido
            #"run_id":"manual__2025-03-02T23:23:59.985491+00:00"
            # tal vez hay que pasar "dag_id"
        }
    import ipdb
    ipdb.set_trace()
    r = requests.post(
        url=url,
        headers=headers,
        files=files,
        data=data
    )

    print_format(r)

def download_latest_manifest(keys_only = True, trimmed = True):
    """Internal Manifest GET"""
    # projects_slug = "balboa-analytics-datacoves"
    projects_slug="analytics-local"
    query_str = f"trimmed={str(trimmed).lower()}"
    r = requests.get(
        # Here we can use get_internal_endpoint
        url=get_external_endpoint(endpoint=f"api/internal/projects/{projects_slug}/latest-manifest?{query_str}"),
        headers=get_headers(token=sa_airflow),
    )

    print_format_json(r,  keys_only)

def print_format_json(r, keys_only = True):
    json_data = r.json()

    if not isinstance(json_data, dict):
        raise ValueError("JSON data is not a dictionary")

    if keys_only:
        top_level_keys = list(json_data.keys())#[:output_keys]
        print(*top_level_keys, sep='\n')

    else:
        formatted_json = json.dumps(json_data, sort_keys=False, indent=2)
        colorful_json = highlight(formatted_json, lexers.JsonLexer(), formatters.TerminalFormatter())
        print(colorful_json)

def print_format(r):
    print("STATUS:", r.status_code, "| RESPONSE:", r.text)

def reload_lib():
    reload(dbt)
    print("Reloaded code!!")


load_dotenv()
base_url_external = os.getenv("DATACOVES__EXTERNAL_URL")
base_url_internal = os.getenv("DATACOVES__UPLOAD_MANIFEST_URL")
sa_user = os.getenv("DATACOVES__SECRETS_TOKEN")

# Airflow SA token
sa_airflow = os.getenv("DATACOVES__UPLOAD_MANIFEST_TOKEN")

internal_bearer_token = os.getenv("DATACOVES__INTERNAL_BEARER_TOKEN")

# Generated public token
sa_public = regen_public_token()

environment_slug = os.getenv("DATACOVES__ENVIRONMENT_SLUG")


if __name__ == "__main__":
    print("=== INIT ===")
    # check_health()
    # upload_manifest()
    download_latest_manifest(trimmed=True)
    print("=== END ===")
