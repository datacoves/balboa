import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Sequence

import requests

from airflow.exceptions import AirflowException
from airflow.operators.bash_operator import BashOperator
from airflow.utils.context import Context

DATACOVES_DEFAULT_VENV = "/opt/datacoves/virtualenvs/main"


class DatacovesBashException(Exception):
    pass


class DatacovesBashOperator(BashOperator):
    """
    Base DatacovesBash class
    Copy the entire Datacoves repo to a temporary directory, then run the command within that directory.
    If no virtual environment is specified, Datacoves default virtual environment is used.
    If the virtual environment does not exist, the command is run without a virtual environment.
    """

    template_fields: Sequence[str] = (
        "bash_command",
        "env",
        "cwd",
        "virtualenv",
        "activate_venv",
    )
    template_fields_renderers = {"bash_command": "bash", "env": "json"}
    template_ext: Sequence[str] = (".sh", ".bash")

    @classmethod
    def _get_full_command(cls, command, virtualenv=None):
        if cls.__name__ == "_DatacovesDbtDecoratedOperator":
            if not Path(os.environ["DATACOVES__DBT_HOME"], "dbt_packages").exists():
                command = f"dbt deps && {command}"
        if virtualenv and Path(virtualenv).exists():
            full_command = f"source {virtualenv}/bin/activate && {command}"
        else:
            activate_cmd = (
                f"source {DATACOVES_DEFAULT_VENV}/bin/activate && "
                if Path(DATACOVES_DEFAULT_VENV).exists()
                else ""
            )
            full_command = f"{activate_cmd}{command}"
        return full_command

    def __init__(
        self,
        bash_command: str = "",
        virtualenv: str = None,
        activate_venv: bool = True,
        cwd: str = None,
        upload_manifest: bool = False,
        *args,
        **kwargs,
    ):
        """
        :param virtualenv: The path to the virtual environment.
        :type virtualenv: str
        :param activate_venv: Whether to activate the virtualenv
        :type activate_venv: bool
        :param bash_command: The bash command to run.
        :type bash_command: str
        :param cwd: The current working directory to run the command, relative to repo root, i.e: 'transform'.
        :type cwd: str
        """
        self.virtualenv = virtualenv
        self.cwd = cwd
        self.activate_venv = activate_venv
        self.upload_manifest = upload_manifest
        self.bash_command = bash_command
        if activate_venv:
            full_command = self._get_full_command(bash_command, virtualenv)
        else:
            full_command = bash_command

        # If there are null (None) environment variables, break
        if "env" in kwargs:
            self.check_env(kwargs["env"])

        super().__init__(bash_command=full_command, cwd=cwd, *args, **kwargs)

    def execute(self, context: Context):
        if self.env:
            self.check_env(self.env)
        repo_path = os.environ["DATACOVES__REPO_PATH"]
        with tempfile.TemporaryDirectory() as tmp_dir:
            subprocess.run(["cp", "-rfT", f"{repo_path}/", tmp_dir], check=False)
            # Add tmp_dir to the Python path
            os.environ["PYTHONPATH"] = f"{tmp_dir}:{os.environ.get('PYTHONPATH')}"
            sys.path.append(tmp_dir)
            if self.cwd:
                self.cwd = f"{tmp_dir}/{self.cwd}"
            else:
                self.cwd = tmp_dir
            super().execute(context)

            if self.upload_manifest and self._get_env("DATACOVES__UPLOAD_MANIFEST"):
                self._upload_manifest()

    def _upload_manifest(self):
        manifest_path = Path(self.cwd, "target/manifest.json")

        if os.path.isfile(manifest_path):
            url = self._get_env("DATACOVES__UPLOAD_MANIFEST_URL")
            bearer_token = self._get_env("DATACOVES__UPLOAD_MANIFEST_TOKEN")
            env_slug = self._get_env("DATACOVES__ENVIRONMENT_SLUG")
            run_id = self._get_env("AIRFLOW_CTX_DAG_RUN_ID")
            print(f"Uploading manifest for DAGRun {run_id} in Environment {env_slug}")
            with open(manifest_path, "r") as file:
                contents = file.read()
                headers = {"Authorization": f"Bearer {bearer_token}"}
                payload = {
                    "environment_slug": env_slug,
                    "run_id": run_id,
                }
                files = {"file": contents}
                res = requests.post(
                    url, headers=headers, data=payload, files=files, timeout=600.0
                )

                if res.ok:
                    print("Manifest upload successful")
                else:
                    raise AirflowException(
                        f"Manifest upload failed with code {res.status_code}: {res.content}"
                    )

        else:
            print("Upload manifest enabled but no manifest.json found. Skipping.")

    def _get_env(self, key):
        return os.environ.get(key)

    def check_env(self, environment):
        # If there are null (None) environment variables, break
        null_vars = self._filter_env(environment, None)
        if null_vars:
            raise DatacovesBashException(
                f"Environment variables not set: {', '.join(null_vars)}"
            )
        # If there are empty ("") environment variables, warn the user in Logs
        empty_vars = self._filter_env(environment, "")
        if empty_vars:
            print(f"Environment variables not set: {', '.join(empty_vars)}")

    def _filter_env(self, environment: Dict[str, Any], filter_value):
        vars = []
        if filter_value is None:
            vars = [key for key, value in environment.items() if value is None]
        else:
            vars = [key for key, value in environment.items() if value == filter_value]
        return vars
