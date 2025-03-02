import json
import os

import requests


class DatacovesDbtAPI:
    def __init__(self):
        self.user_token = os.getenv("DATACOVES__SECRETS_TOKEN")
        self.airflow_token = os.getenv("DATACOVES__UPLOAD_MANIFEST_TOKEN")
        base_url_internal = os.getenv("DATACOVES__UPLOAD_MANIFEST_URL")
        base_url_external = os.getenv("DATACOVES__EXTERNAL_URL")
        use_external_url = self._str_to_bool(
            os.getenv("DATACOVES__USE_EXTERNAL_URL", "false")
        )
        self.base_url = base_url_external if use_external_url else base_url_internal
        self.environment_slug = os.getenv("DATACOVES__ENVIRONMENT_SLUG")
        self.project_slug = os.getenv("DATACOVES__PROJECT_SLUG")
        self.download_successful = False

        self.user_headers = {
            "Authorization": f"Bearer {self.user_token}",
            "Accept": "application/json",
        }
        self.airflow_headers = {
            "Authorization": f"Bearer {self.airflow_token}",
            "Accept": "application/json",
        }

    def _str_to_bool(self, s: str) -> bool:
        return s.lower() in ("true", "1", "yes", "y")

    def get_endpoint(self, endpoint: str) -> str:
        return f"{self.base_url}/{endpoint}"

    def api_call(
        self,
        method: str,
        endpoint: str,
        headers: dict,
        data: dict = {},
        files: dict = None,
    ):
        try:
            breakpoint()
            url = self.get_endpoint(endpoint)
            response = requests.request(method, url, headers=headers, data=data, files=files)
            response.raise_for_status()
            return response
        except Exception:
            response_errors = response.json().get("errors")
            raise Exception(response_errors)

    def upload_latest_manifest(
        self,
        env_slug: str,
        run_id: str,
        files_payload,
    ):
        res = self.api_call(
            "POST",
            "api/internal/manifests",
            headers=self.airflow_headers,
            files=files_payload,
            data={
                "environment_slug": env_slug,
                "run_id": run_id
            }
        )
        if res.ok:
            print("Manifest uploaded successfully")
        else:
            print("Error uploading manifest")

    def download_latest_manifest(
        self,
        trimmed=True,
        destination=f"{os.getenv('DATACOVES__REPO_PATH')}/transform/target/manifest.json",
    ):
        query_str = f"trimmed={str(trimmed).lower()}"
        res = self.api_call(
            "GET",
            f"api/internal/projects/{self.project_slug}/latest-manifest?{query_str}",
            headers=self.airflow_headers,
        )
        if res.ok:
            manifest = res.json()
            with open(destination, "w") as f:
                json.dump(manifest, f, indent=4)
            print(f"Downloaded manifest to {destination}")
        else:
            print("Error downloading manifest")

    def download_file_by_tag(self, tag: str, destination: str):
        params = f"tag={tag}"
        res = self.api_call(
            "GET",
            f"api/internal/environments/{self.environment_slug}/files?{params}",
            headers=self.airflow_headers,
        )
        if res.ok:
            content = res.json().get("data", {}).get("contents", "")
            with open(destination, "wb") as f:
                f.write(content)
            print(f"Downloaded {destination}")
            self.download_successful = True
        else:
            print(f"Error downloading {destination}")
            self.download_successful = False

    def upload_files(self, files: dict):
        self.api_call(
            "POST",
            f"api/internal/environments/{self.environment_slug}/files",
            headers=self.airflow_headers,
            files=files,
        )
        print("Files uploaded successfully")
