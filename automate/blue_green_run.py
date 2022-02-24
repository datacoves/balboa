#!/usr/bin/env python3
import argparse
import subprocess
# import sys
import os
# import time
import logging


# DBT_PROJECT_DIR = "/opt/airflow/dags/repo/balboa.git/transform"
DBT_FINAL_DB_NAME = os.environ['DBT_DATABASE']

#This db must be prefaced 'staging' to work with /transform/macros/ref.sql override
DBT_STAGING_DB_NAME = "staging_" + os.environ['DBT_DATABASE'] 

DBT_HOME = os.environ.get("DBT_HOME", os.environ.get("DATACOVES__DBT_HOME"))

def get_commit_hash():
    return subprocess.run(['git', 'rev-parse', 'HEAD'],
        capture_output=True,
        text=True,
        cwd=DBT_HOME).stdout.strip("\n")

def run_dbt(args, cwd):
    if args.is_production:
        if args.selector:
            logging.info("Running dbt build with selector " + args.selector +"+")
            subprocess.run(["dbt", "build","--fail-fast","-s", f"{args.selector}+"], check=True, cwd=cwd)
        else:
            logging.info("Production run of dbt")
            subprocess.run(["dbt", "build","--fail-fast"], check=True, cwd=cwd)
    else:
        logging.info("Getting prod manifest")

        subprocess.run(["../automate/dbt/get_artifacts.sh"], check=True, cwd=cwd)
        
        logging.info("Deployment run of dbt")
        subprocess.run(["dbt", "build", "--defer", "--fail-fast",'--state', 'logs', '-s', 'state:modified+'], check=True, cwd=cwd)

    # Save the latest manifest to snowflake
    subprocess.run(["dbt", "compile"], check=True, cwd=cwd)
    logging.info("Uploading new prod manifest")

    subprocess.run(["dbt", "--no-write-json", "run-operation", "upload_artifacts"], check=True, cwd=cwd)

def main(args):
    """
    Runs dbt build
    """
    logging.info("\n\n===== STARTING BLUE / GREEN RUN =====\n")
    commit_hash = get_commit_hash()

    if args.is_production:
        cwd = f"/home/airflow/transform-pr-{commit_hash}"
        logging.info("Copying dbt project to temp directory")
        subprocess.run(["cp", "-rf", DBT_HOME, cwd], check=True)
    else:
        cwd = DBT_HOME
        logging.info("DBT_HOME " + DBT_HOME)

    logging.info("Setting db to staging database")
    os.environ['DBT_DATABASE'] = DBT_STAGING_DB_NAME

    logging.info("Checking that staging database does not exist")
    STAGING_DB_ARGS = '{"db_name": "' + DBT_STAGING_DB_NAME + '"}'
    logging.info(STAGING_DB_ARGS)
    subprocess.run(["dbt", "--no-write-json", "run-operation", "check_db_does_not_exist", "--args", STAGING_DB_ARGS], check=True, cwd=cwd)

    CLONE_DB_ARGS = '{"source_db": "' + DBT_FINAL_DB_NAME + '", "target_db": "' + DBT_STAGING_DB_NAME + '"}'
    subprocess.run(["dbt", "run-operation", "clone_database", "--args", CLONE_DB_ARGS], check=True, cwd=cwd)

    # this is here because cloning of db does not clone the stage
    logging.info("Creating stage for dbt_aritifacts")
    subprocess.run(["dbt", "run-operation", "create_dbt_artifacts_stage"], check=True, cwd=cwd)

    run_dbt(args, cwd)

    logging.info("Granting usage to staging database ")
    subprocess.run(["dbt", "run-operation", "grant_prd_usage"], check=True, cwd=cwd)

    logging.info("Swapping staging database " + DBT_STAGING_DB_NAME + " with production " + DBT_FINAL_DB_NAME)
    SWAP_DB_ARGS = '{"db1": "' + DBT_FINAL_DB_NAME + '", "db2": "' + DBT_STAGING_DB_NAME + '"}'
    subprocess.run(["dbt", "run-operation", "swap_database", "--args", SWAP_DB_ARGS], check=True, cwd=cwd)
    
    if args.is_production:
        logging.info("Removing dbt project temp directory")
        subprocess.run(["rm", "-rf", cwd], check=True)

    logging.info("Dropping staging database")
    subprocess.run(["dbt", "run-operation", "drop_staging_db", "--args", STAGING_DB_ARGS], check=True, cwd=cwd)
    logging.info("done with dropping!!!!")



if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

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
        logging.info(ex)
        exit(1)