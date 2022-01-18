#!/usr/bin/env python3
import subprocess
import sys
import os
import time


DBT_PROJECT_DIR = "/opt/airflow/dags/repo/balboa.git/transform"
DBT_PROD_DB_NAME = "balboa"
DBT_STAGING_DB_NAME = "staging_balboa" #This db must be prefaced 'staging' to work with /transform/macros/ref.sql override
DBT_STAGING_PROFILE_NAME = "staging" #This profile must be set up in prod to point to the staging db


def get_commit_hash():
    return subprocess.run(['git', 'rev-parse', 'HEAD'],
        capture_output=True,
        text=True,
        cwd=DBT_PROJECT_DIR).stdout.strip("\n")


def main(args):
    """
    Runs dbt build
    """
    tag = args[0] if args else ""
    commit_hash = get_commit_hash()
    cwd = f"/home/airflow/transform-pr-{commit_hash}"

    print("Copying dbt project to temp directory")
    subprocess.run(["cp", "-rf", DBT_PROJECT_DIR, cwd], check=True)

    print("Loading dbt dependencies")
    subprocess.run(["dbt", "deps"], check=True, cwd=cwd)

    print("Setting db to staging database")
    subprocess.run(["export", "DBT_DATABASE="+DBT_STAGING_DB_NAME], check=True, cwd=cwd)
    
    print("Checking that staging database does not exist")
    while 1:
        try:
            subprocess.run(["dbt", "--no-write-json", "run-operation", "check_db_does_not_exist", "--args", "'{db_name: "+DBT_STAGING_DB_NAME+"}'"], check=True, cwd=cwd)
            break
        except:
            print("Staging database "+DBT_STAGING_DB_NAME+" exists, waiting 60 seconds to try again")
            time.sleep(60)

    print("Cloning db to 'staging'")
    subprocess.run(["dbt", "run-operation", "clone_database", "--args", "'{source_db: "+DBT_PROD_DB_NAME+", target_db: "+DBT_STAGING_DB_NAME+"}'"], check=True, cwd=cwd)

    if tag: #If specific run is requested
        print("Running dbt build "+tag+"+")
        subprocess.run(["dbt", "build", "-s", f"tag:{tag}+"], check=True, cwd=cwd)
    else:
        print("Getting prod manifest")
        subprocess.run(["export", "DBT_HOME="+cwd], check=True, cwd=cwd) #this env_var is referenced by get_artifacts
        subprocess.run(["/opt/airflow/dags/repo/balboa.git/automate/dbt/get_artifacts.sh"], check=True, cwd=cwd)
        print("Building all changes and their dependencies")
        subprocess.run(["dbt", "build", '--state', 'logs', '-s', 'state:modified+'], check=True, cwd=cwd)

    print("Swapping staging database "+DBT_STAGING_DB_NAME+" with production "+DBT_PROD_DB_NAME)
    subprocess.run(["dbt", "run-operation", "swap_database", "--args", "'{db1: "+DBT_PROD_DB_NAME+", db2: "+DBT_STAGING_DB_NAME+"}'"], check=True, cwd=cwd)
    
    print("Uploading new prod manifest")
    subprocess.run(["dbt", "--no-write-json", "run-operation", "upload_manifest_catalog"], check=True, cwd=cwd)

    print("Removing dbt project temp directory")
    subprocess.run(["rm", "-rf", cwd], check=True)

if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except Exception as ex:
        print(ex)
        exit(1)

