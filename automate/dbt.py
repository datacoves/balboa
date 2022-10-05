#!/usr/bin/env python3

import logging
import os
import shlex
import subprocess
import sys
import tempfile


def main(args: list):
    help(args)
    logging.basicConfig(
        format="%(levelname)s: %(message)s",
        level=logging.DEBUG if "--debug" in args else logging.INFO,
    )
    project_dir = get_project_dir(args)
    if not project_dir:
        project_dir = os.environ.get(
            "DBT_PROJECT_DIR", os.environ.get("DATACOVES__DBT_HOME")
        )
    if not project_dir:
        print(
            "No dbt project specified, 'DBT_PROJECT_DIR', 'DATACOVES__DBT_HOME' vars or '--project-dir' argument should be set."
        )
        exit(1)
    logging.debug(f"project-dir: {project_dir}")
    logging.debug(f"Args to send to dbt: {args}")

    if is_readonly(project_dir):
        with tempfile.TemporaryDirectory() as tmp_dir:
            logging.info(
                f"Readonly project detected. Copying it to temp directory {tmp_dir}"
            )
            subprocess.run(["cp", "-rf", f"{project_dir}/", tmp_dir], check=True)
            run_dbt(args, cwd=tmp_dir)
    else:
        run_dbt(args, cwd=project_dir)


def help(args):
    if "--help" in args:
        print(
            "\n".join(
                [
                    "",
                    "[Datacoves dbt runner]",
                    "",
                    " Use this command to run dbt commands on special environments such as Airflow, or CI workers.",
                    " - If a read-only dbt project is detected it is previously copied to a temp dir.",
                    " - If `packages` folder is empty, it runs `dbt deps` before running the actual command.",
                    "",
                    ' Accepts same arguments as `dbt` i.e. `./dbt.py run --vars \'{"key": "value"}\'`',
                    "",
                ]
            )
        )
        exit(0)


def run_dbt(args: list, cwd: str):
    """Run dbt command on a specific directory passing received arguments. Runs dbt deps if missing packages"""
    if not os.path.exists(os.path.join(cwd, "dbt_packages")) and not os.path.exists(
        os.path.join(cwd, "dbt_modules")
    ):
        run_command(f"dbt deps", cwd=cwd)
    str_args = " ".join([arg if " " not in arg else f"'{arg}'" for arg in args])
    run_command(f"dbt {str_args}", cwd=cwd)


def get_project_dir(args: list):
    """Returns project-dir arg from args list, and removes it from the list"""
    try:
        idx = args.index("--project-dir")
        project_dir = args[idx + 1]
        if project_dir:
            args.pop(idx)
            args.pop(idx)
        return project_dir
    except (IndexError, ValueError):
        return None


def is_readonly(folder: str) -> bool:
    """Returns True if `folder` is readonly"""
    stat = os.statvfs(folder)
    return bool(stat.f_flag & os.ST_RDONLY) or not os.access(folder, os.W_OK)


def run_command(
    command: str,
    env=None,
    env_path=None,
    **kwargs,
):
    """Activates a python environment if found and runs a command using it"""
    if not env:
        env = os.environ.copy()
    if not env_path:
        env_path = os.environ.get(
            "DATACOVES__VIRTUALENV_PATH", "/opt/datacoves/virtualenvs/main"
        )

    if os.path.exists(env_path):
        cmd_list = shlex.split(
            f"/bin/bash -c 'source {env_path}/bin/activate && {command}'"
        )
    else:
        cmd_list = shlex.split(command)

    logging.info(f"Running: {' '.join(cmd_list)}")
    return subprocess.run(cmd_list, env=env, **kwargs)


if __name__ == "__main__":
    main(sys.argv[1:])
