#!/usr/bin/env python3
import subprocess
import sys
import os


def main(args):
    """
    Runs dbt build
    """
    tag = args[0] if args else ""
    cwd = "/home/airflow/transform"
    
    subprocess.run(["rm", "-rf", cwd], check=True)

    subprocess.run(["cp", "-rf", "/opt/airflow/dags/repo/balboa.git/transform", cwd], check=True)

    subprocess.run(["dbt", "deps"], check=True, cwd=cwd)

    if tag:
        subprocess.run(["dbt", "build", "-s", "tag:{tag}"], check=True, cwd=cwd)
    else:
        subprocess.run(["dbt", "build"], check=True, cwd=cwd)

    # subprocess.run(["dbt", "run-operation", "upload_dbt_artifacts",
    #     "--args", "{filenames: [manifest, run_results]}"], check=True, cwd=cwd)


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except Exception as ex:
        print(ex)
        exit(1)
