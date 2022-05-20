#!/usr/bin/env python3
import argparse
import subprocess
import os
import logging
import shlex

DBT_FINAL_DB_NAME = os.environ['DBT_DATABASE']

#This db must be prefaced 'staging' to work with /transform/macros/ref.sql override
DBT_STAGING_DB_NAME = "staging_" + os.environ['DBT_DATABASE'] 

DBT_HOME = os.environ.get("DBT_HOME", os.environ.get("DATACOVES__DBT_HOME"))

VIRTUALENV_PATH = "/home/airflow/.virtualenvs/datacoves"


def get_commit_hash():
    return subprocess.run(['git', 'rev-parse', 'HEAD'],
        capture_output=True,
        text=True,
        cwd=DBT_HOME).stdout.strip("\n")


def run_venv_command(command: str, cwd: str=None):
    cmd_list = shlex.split(f"/bin/bash -c 'source {VIRTUALENV_PATH}/bin/activate && {command}'")
    process = subprocess.run(cmd_list, check=True, cwd=cwd)


def run_dbt(args, cwd):
    if args.is_production:
        if args.selector:
            logging.info("Running dbt build with selector " + args.selector +"+")
            run_venv_command(f"dbt build --fail-fast -s {args.selector}+", cwd=cwd)
        else:
            logging.info("Production run of dbt")
            run_venv_command("dbt build --fail-fast", cwd=cwd)
    else:
        logging.info("Getting prod manifest")
        # this env_var is referenced by get_artifacts
        os.environ['DBT_HOME'] = cwd
        run_venv_command("../automate/dbt/get_artifacts.sh", cwd=cwd)
        
        logging.info("Deployment run of dbt")
        run_venv_command("dbt build --defer --fail-fast --state logs -s state:modified+", cwd=cwd)

    # Save the latest manifest to snowflake
    run_venv_command("dbt compile", cwd=cwd)
    logging.info("Uploading new prod manifest")

    logging.info("Running dbt against Database: " + os.environ['DBT_DATABASE'])

    run_venv_command("dbt --no-write-json run-operation upload_artifacts", cwd=cwd)
    run_venv_command("dbt --no-write-json run-operation upload_dbt_artifacts_v2", cwd=cwd)

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
        run_venv_command("dbt deps", cwd=cwd)
    else:
        cwd = DBT_HOME
        logging.info("DBT_HOME " + DBT_HOME)

    logging.info("Setting db to staging database")
    os.environ['DBT_DATABASE'] = DBT_STAGING_DB_NAME
    logging.info("Running dbt against Database: " + os.environ['DBT_DATABASE'])

    logging.info("Checking that staging database does not exist")
    STAGING_DB_ARGS = '{"db_name": "' + DBT_STAGING_DB_NAME + '"}'
    logging.info(STAGING_DB_ARGS)
    run_venv_command(f"dbt --no-write-json run-operation check_db_does_not_exist --args \"{STAGING_DB_ARGS}\"", cwd=cwd)

    CLONE_DB_ARGS = '{"source_db": "' + DBT_FINAL_DB_NAME + '", "target_db": "' + DBT_STAGING_DB_NAME + '"}'
    run_venv_command(f"dbt run-operation clone_database --args \"{CLONE_DB_ARGS}\"", cwd=cwd)

    # this is here because cloning of db does not clone the stage
    logging.info("Creating stage for dbt_aritifacts")
    run_venv_command("dbt run-operation create_dbt_artifacts_stage", cwd=cwd)

    run_dbt(args, cwd)

    logging.info("Granting usage to staging database ")
    run_venv_command("dbt run-operation grant_prd_usage", cwd=cwd)

    logging.info("Swapping staging database " + DBT_STAGING_DB_NAME + " with production " + DBT_FINAL_DB_NAME)
    SWAP_DB_ARGS = '{"db1": "' + DBT_FINAL_DB_NAME + '", "db2": "' + DBT_STAGING_DB_NAME + '"}'
    run_venv_command(f"dbt run-operation swap_database --args \"{SWAP_DB_ARGS}\"", cwd=cwd)
    
    logging.info("Dropping staging database")
    run_venv_command(f"dbt run-operation drop_staging_db --args \"{STAGING_DB_ARGS}\"", cwd=cwd)
    logging.info("done with dropping!!!!")

    if args.is_production:
        logging.info("Removing dbt project temp directory")
        subprocess.run(["rm", "-rf", cwd], check=True)


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