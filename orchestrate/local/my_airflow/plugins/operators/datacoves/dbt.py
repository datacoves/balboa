import os
from pathlib import Path
from typing import Sequence

from operators.datacoves.bash import DatacovesBashOperator


class DatacovesDbtOperator(DatacovesBashOperator):
    """
    Copy the entire Datacoves repo to a temporary directory
    Look for dbt project inside that temp and use as 'cwd'
    Run 'dbt deps' and 'command' in 'cwd' (activate virtualenv passed)
    """

    template_fields = (
        "bash_command",
        "env",
        "virtualenv",
        "project_dir",
    )
    template_fields_renderers = {"bash_command": "bash", "env": "json"}
    template_ext: Sequence[str] = (".sh", ".bash")

    def __init__(
        self,
        project_dir: str = None,
        virtualenv: str = None,
        bash_command: str = "",
        run_dbt_deps: bool = False,
        upload_manifest=True,
        *args,
        **kwargs,
    ):
        """
        :param project_dir: optional relative path of the project to use
        (it'll be discovered using Datacoves Repo and Project environment variables)
        :type project_dir: str
        :param virtualenv: optional path to a virtual environment.
        (Datacoves default Airflow virtualenv will be used instead)
        :type virtualenv: str
        :param bash_command: The bash command to run.
        :type bash_command: str
        :param run_dbt_deps: Whether to force dbt deps running
        :type run_dbt_deps: bool
        """
        self.bash_command = bash_command
        if project_dir:
            self.project_dir = Path(project_dir)
        else:
            # relpath from repo root to dbt project
            self.project_dir = Path(os.environ["DATACOVES__DBT_HOME"]).relative_to(
                os.environ["DATACOVES__REPO_PATH_RO"]
            )
        if (
            run_dbt_deps
            or not Path(os.environ["DATACOVES__DBT_HOME"], "dbt_packages").exists()
        ):
            bash_command = f"dbt deps && {bash_command}"

        super().__init__(
            bash_command=bash_command,
            virtualenv=virtualenv,
            cwd=self.project_dir,
            upload_manifest=upload_manifest,
            *args,
            **kwargs,
        )
