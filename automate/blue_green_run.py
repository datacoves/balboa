#!/usr/bin/env python
import argparse
import logging
import os
import shlex
import subprocess
import tempfile

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)

DBT_FINAL_DB_NAME = os.environ.get("DATACOVES__MAIN__DATABASE", 'Database ENV VAR not set')
# This db must be prefaced 'staging' to work with /transform/macros/ref.sql override
DBT_STAGING_DB_NAME = DBT_FINAL_DB_NAME + '_STAGING'
os.environ["DATACOVES__MAIN__DATABASE"] = DBT_STAGING_DB_NAME

DBT_HOME = os.environ.get("DATACOVES__DBT_HOME")

VIRTUALENV_PATH = "/opt/datacoves/virtualenvs/main"

DBT_COVES__CLONE_PATH = tempfile.NamedTemporaryFile().name

def main(is_ci_cd_run: bool = False, selector: str = None, target: str = None):
    """
    Manages blue/green deployment workflow
    """

    logging.info("\n\n===== STARTING BLUE / GREEN RUN =====\n")

    dbt_target = ''

    if target:
        dbt_target = f" -t {target}"

    #####
    logging.info("\n\n===== REMOVE THIS =====\n")
    STAGING_DB_ARGS = '{"db_name": "' + DBT_STAGING_DB_NAME + '"}'
    run_command(f'dbt-coves dbt -- run-operation drop_staging_db --args "{STAGING_DB_ARGS}" {dbt_target}')
    ######

    logging.info("Checking that staging database does not exist")
    STAGING_DB_ARGS = '{"db_name": "' + DBT_STAGING_DB_NAME + '"}'
    logging.info(STAGING_DB_ARGS)
    run_command(
        f'dbt-coves dbt -- --no-write-json run-operation check_db_does_not_exist --args "{STAGING_DB_ARGS}" {dbt_target}'
    )

    CLONE_DB_ARGS = (
        '{"source_db": "'
        + DBT_FINAL_DB_NAME
        + '", "target_db": "'
        + DBT_STAGING_DB_NAME
        + '"}'
    )
    run_command(f'dbt-coves dbt -- run-operation clone_database --args "{CLONE_DB_ARGS}" {dbt_target}')

    # Performs the dbt run
    run_dbt(selector=selector, dbt_target=dbt_target, is_ci_cd_run=is_ci_cd_run)

    logging.info("Granting usage to staging database ")
    USAGE_ARGS = (
        '{"db_name": "'
        + DBT_STAGING_DB_NAME
        + '"}'
    )
    run_command(f'dbt-coves dbt -- run-operation grant_prd_usage --args "{USAGE_ARGS}" {dbt_target}')

    logging.info(
        "Swapping staging database "
        + DBT_STAGING_DB_NAME
        + " with production "
        + DBT_FINAL_DB_NAME
    )
    SWAP_DB_ARGS = (
        '{"db1": "' + DBT_FINAL_DB_NAME + '", "db2": "' + DBT_STAGING_DB_NAME + '"}'
    )
    run_command(f'dbt-coves dbt -- run-operation swap_database --args "{SWAP_DB_ARGS}" {dbt_target}')

    logging.info("Dropping staging database")
    run_command(f'dbt-coves dbt -- run-operation drop_staging_db --args "{STAGING_DB_ARGS}" {dbt_target}')
    logging.info("done with dropping!!!!")

    # Save the latest manifest to snowflake stage
    os.environ["DATACOVES__MAIN__DATABASE"] = DBT_FINAL_DB_NAME
    run_command(f"dbt-coves dbt -- compile {dbt_target}")

    os.environ["DATACOVES__MAIN__DATABASE"] = DBT_FINAL_DB_NAME

    logging.info("Uploading new prod manifest")
    run_command(f"dbt-coves dbt -- --no-write-json run-operation upload_artifacts {dbt_target}")


def run_dbt(selector: str = None, dbt_target: str = None, is_ci_cd_run: bool = False):
    """
    Runs dbt build and uploads artifacts
    """
    # NOTE: you must have gotten the prod manifest in a step prior to this
    # we set an env variable MANIFEST_FOUND when we get the manifest
    MANIFEST_FOUND = os.environ.get("MANIFEST_FOUND", "false")

    logging.info("MANIFEST_FOUND = " + MANIFEST_FOUND)

    if selector and is_ci_cd_run:
        print("CI and Selector provided, will use selector and not Slim CI")
        selector = f"-s {selector}"
    elif selector:
        selector = f"-s {selector}"
    elif is_ci_cd_run and MANIFEST_FOUND == "true":
        selector = f"--defer --state logs -s state:modified+"
    else:
        selector = ''

    dbt_command = f"dbt-coves dbt -- build --fail-fast {selector} {dbt_target}"
    print(f"Running dbt command: \n{dbt_command}")
    run_command(dbt_command)

def run_command(command: str, capture_output=False):
    my_env = os.environ.copy()

    my_env["DBT_COVES__CLONE_PATH"] = DBT_COVES__CLONE_PATH

    if os.path.exists(VIRTUALENV_PATH):
        """Activates a python environment and runs a command using it"""
        cmd_list = shlex.split(
            f"/bin/bash -c 'source {VIRTUALENV_PATH}/bin/activate && {command}'"
        )
    else:
        cmd_list = shlex.split(command)

    return subprocess.run(
        cmd_list, env=my_env, check=True, capture_output=capture_output
    )


if __name__ == "__main__":

    try:
        parser = argparse.ArgumentParser(
            description="Used to run dbt, with blue/green steps during deployment or in production.",
        )

        parser.add_argument(
            "-s",
            "--select",
            dest="selector",
            action="store",
            help="Specify the dbt selector to use during the run",
        )

        parser.add_argument(
            "-t",
            "--target",
            dest="target",
            action="store",
            help="Specify the dbt target to use during the run",
        )

        parser.add_argument(
            "--ci-cd-run",
            dest="is_ci_cd_run",
            action="store_true",
            help="Defines if the run is a ci/cd deployment run",
        )

        args = vars(parser.parse_args())

        main(**args)

    except Exception as ex:
        logging.info(ex)
        exit(1)
