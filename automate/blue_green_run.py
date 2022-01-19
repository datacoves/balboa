#!/usr/bin/env python3
import argparse
import subprocess
# import sys
import os
# import time
import logging


DBT_PROJECT_DIR = "/opt/airflow/dags/repo/balboa.git/transform"
DBT_FINAL_DB_NAME = os.environ['DBT_DATABASE']

#This db must be prefaced 'staging' to work with /transform/macros/ref.sql override
DBT_STAGING_DB_NAME = "staging_" + os.environ['DBT_DATABASE'] 

def get_commit_hash():
    return subprocess.run(['git', 'rev-parse', 'HEAD'],
        capture_output=True,
        text=True,
        cwd=DBT_PROJECT_DIR).stdout.strip("\n")

def run_dbt(args):
    if args.is_production:
        if args.selector:
            logging.warning("Running dbt build with selector " + args.selector +"+")
            subprocess.run(["dbt", "build", "-s", f"{args.selector}+"], check=True, cwd=cwd)
        else:
            logging.warning("Production run of dbt")
            subprocess.run(["dbt", "build"], check=True, cwd=cwd)
    else:
        logging.warning("Getting prod manifest")
        # this env_var is referenced by get_artifacts
        os.environ['DBT_HOME'] = cwd
        subprocess.run(["/opt/airflow/dags/repo/balboa.git/automate/dbt/get_artifacts.sh"], check=True, cwd=cwd)
        
        logging.warning("Deployment run of dbt")
        subprocess.run(["dbt", "build", '--state', 'logs', '-s', 'state:modified+'], check=True, cwd=cwd)

    subprocess.run(["dbt", "compile"], check=True, cwd=cwd)
    logging.warning("Uploading new prod manifest")
    subprocess.run(["dbt", "--no-write-json", "run-operation", "upload_manifest_catalog"], check=True, cwd=cwd)


def main(args):
    """
    Runs dbt build
    """
    print(args.selector)
    print(args.is_production)

    commit_hash = get_commit_hash()
    cwd = f"/home/airflow/transform-pr-{commit_hash}"

    logging.warning("Copying dbt project to temp directory")
    subprocess.run(["cp", "-rf", DBT_PROJECT_DIR, cwd], check=True)

    logging.warning("Loading dbt dependencies")
    subprocess.run(["dbt", "deps"], check=True, cwd=cwd)

    logging.warning("Setting db to staging database")
    os.environ['DBT_DATABASE'] = DBT_STAGING_DB_NAME
    
#     logging.warning("Checking that staging database does not exist")
#     while 1:
#         try:
#             subprocess.run(["dbt", "--no-write-json", "run-operation", "check_db_does_not_exist", "--args", '{"db_name": "%d"}' % (DBT_STAGING_DB_NAME)], check=True, cwd=cwd)
#             break
#         except:
#             logging.warning("Staging database "+DBT_STAGING_DB_NAME+" exists, waiting 60 seconds to try again")
#             time.sleep(60)

    logging.warning("Cloning db to 'staging'")
    subprocess.run(["dbt", "run-operation", "clone_database", "--args", "'{source_db: " + DBT_FINAL_DB_NAME +
        ", target_db: " + DBT_STAGING_DB_NAME + "}'"], check=True, cwd=cwd)

    run_dbt(args)

    logging.warning("Swapping staging database " + DBT_STAGING_DB_NAME + " with production " + DBT_FINAL_DB_NAME)
    subprocess.run(["dbt", "run-operation", "swap_database", "--args", "'{db1: "+DBT_FINAL_DB_NAME + ", db2: " + DBT_STAGING_DB_NAME+"}'"], check=True, cwd=cwd)
    
    logging.warning("Removing dbt project temp directory")
    subprocess.run(["rm", "-rf", cwd], check=True)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(
            description='Used to run dbt, with blue/green steps during deployment or in produciton.',
            epilog='--deployment-run and --production-run are mutually exclusive')
        
        parser.add_argument('-s','--select', dest='selector', action='store',
                            help='Specify the dbt selector to use during the run')

        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('--deployment-run', 
                            dest='is_production', 
                            action='store_false',
                            help='Defines if the run is a Deployment run')
        group.add_argument('--production-run', 
                            dest='is_production', 
                            action='store_true',
                            help='Defines if the run is a Production run')

        args = parser.parse_args()
        main(args)

    except Exception as ex:
        logging.warning(ex)
        exit(1)