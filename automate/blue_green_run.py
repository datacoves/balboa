#!/usr/bin/env python3
import argparse
import subprocess
import os
import logging
import shlex

DBT_FINAL_DB_NAME = os.environ["DBT_DATABASE"]

# This db must be prefaced 'staging' to work with /transform/macros/ref.sql override
DBT_STAGING_DB_NAME = "staging_" + os.environ["DBT_DATABASE"]

DBT_HOME = os.environ.get("DBT_HOME", os.environ.get("DATACOVES__DBT_HOME"))

VIRTUALENV_PATH = "/opt/datacoves/virtualenvs/main"


def main(is_production: bool = False, selector: str = None):
    """
    Manages blue/green deployment workflow
    """
    logging.info("\n\n===== STARTING BLUE / GREEN RUN =====\n")
    commit_hash = get_commit_hash()

    if is_production:
        cwd = f"/home/airflow/transform-pr-{commit_hash}"
        logging.info("Copying dbt project to temp directory")
        subprocess.run(["cp", "-rf", DBT_HOME, cwd], check=True)
        run_command("dbt deps")
    else:
        cwd = DBT_HOME
        logging.info("DBT_HOME " + DBT_HOME)

    logging.info("Setting db to staging database")
    os.environ["DBT_DATABASE"] = DBT_STAGING_DB_NAME
    logging.info("Running dbt against Database: " + os.environ["DBT_DATABASE"])

    logging.info("Checking that staging database does not exist")
    STAGING_DB_ARGS = '{"db_name": "' + DBT_STAGING_DB_NAME + '"}'
    logging.info(STAGING_DB_ARGS)
    run_command(
        f'dbt --no-write-json run-operation check_db_does_not_exist --args "{STAGING_DB_ARGS}"'
    )

    CLONE_DB_ARGS = (
        '{"source_db": "'
        + DBT_FINAL_DB_NAME
        + '", "target_db": "'
        + DBT_STAGING_DB_NAME
        + '"}'
    )
    run_command(f'dbt run-operation clone_database --args "{CLONE_DB_ARGS}"')

    # this is here because cloning of db does not clone the stage
    logging.info("Creating stage for dbt_aritifacts")
    run_command("dbt run-operation create_dbt_artifacts_stage")

    run_dbt(cwd, is_production=is_production, selector=selector)

    logging.info("Granting usage to staging database ")
    run_command("dbt run-operation grant_prd_usage")

    logging.info(
        "Swapping staging database "
        + DBT_STAGING_DB_NAME
        + " with production "
        + DBT_FINAL_DB_NAME
    )
    SWAP_DB_ARGS = (
        '{"db1": "' + DBT_FINAL_DB_NAME + '", "db2": "' + DBT_STAGING_DB_NAME + '"}'
    )
    run_command(f'dbt run-operation swap_database --args "{SWAP_DB_ARGS}"')

    logging.info("Dropping staging database")
    run_command(f'dbt run-operation drop_staging_db --args "{STAGING_DB_ARGS}"')
    logging.info("done with dropping!!!!")

    if is_production:
        logging.info("Removing dbt project temp directory")
        subprocess.run(["rm", "-rf", cwd], check=True)


def run_dbt(cwd: str, is_production: bool = False, selector: str = None):
    """
    Runs dbt build and uploads artifacts
    """
    if is_production:
        if selector:
            logging.info("Running dbt build with selector " + selector + "+")
            run_command(f"dbt build --fail-fast -s {selector}+")
        else:
            logging.info("Production run of dbt")
            run_command("dbt build --fail-fast")
    else:
        logging.info("Getting prod manifest")
        # this env_var is referenced by get_artifacts
        os.environ["DBT_HOME"] = cwd

        logging.info("BEFORE get_artifacts")
        run_command("../automate/dbt/get_artifacts.sh")
        logging.info("AFTER get_artifacts")

        # we set a return code to MANIFEST_FOUND when we get the manifest and get it here
        MANIFEST_FOUND = os.environ["MANIFEST_FOUND"]

        logging.info("MANIFEST_FOUND = " + MANIFEST_FOUND)
 
        if MANIFEST_FOUND == 1:
            logging.info("Slim deployment run of dbt")
            run_command("dbt build --defer --fail-fast --state logs -s @state:modified")
        else:
            logging.info("Full deployment run of dbt")
            run_command("dbt build --fail-fast")

    # Save the latest manifest to snowflake
    run_command("dbt compile")
    logging.info("Uploading new prod manifest")

    logging.info("Running dbt against Database: " + os.environ["DBT_DATABASE"])

    run_command("dbt --no-write-json run-operation upload_artifacts")
    run_command("dbt --no-write-json run-operation upload_dbt_artifacts_v2")


def get_commit_hash():
    """Gets current commit hash"""
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=DBT_HOME
    ).stdout.strip("\n")


def run_command(command: str, cwd: str = None, capture_output=False):
    if os.path.exists(VIRTUALENV_PATH):
        """Activates a python environment and runs a command using it"""
        cmd_list = shlex.split(
            f"/bin/bash -c 'source {VIRTUALENV_PATH}/bin/activate && {command}'"
        )
    else:
        cmd_list = shlex.split(command)

    return subprocess.run(cmd_list, check=True, capture_output=capture_output)


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)

    try:
        parser = argparse.ArgumentParser(
            description="Used to run dbt, with blue/green steps during deployment or in produciton.",
            epilog="--deployment-run and --production-run are mutually exclusive",
        )

        parser.add_argument(
            "-s",
            "--select",
            dest="selector",
            action="store",
            help="Specify the dbt selector to use during the run",
        )

        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--deployment-run",
            dest="is_production",
            action="store_false",
            help="Defines if the run is a Deployment run",
        )
        group.add_argument(
            "--production-run",
            dest="is_production",
            action="store_true",
            help="Defines if the run is a Production run",
        )

        args = vars(parser.parse_args())

        main(**args)

    except Exception as ex:
        logging.info(ex)
        exit(1)
