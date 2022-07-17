#!/usr/bin/env python3

import glob
import json
import subprocess
import os
import yaml
import argparse
import logging

from pathlib import Path

PROJECT_ROOT_PATH = os.getcwd()

DBT_MACRO_NAME_ROLE_COMPARISON = "snowflake_role_comparison"
DBT_MACRO_NAME_SCHEMA_COMPARISON = "snowflake_schema_comparison"
DBT_MACRO_NAME_WAREHOUSE_COMPARISON = "snowflake_warehouse_comparison"

permifrost_roles = list()
permifrost_schemas = list()
permifrost_warehouses = list()

def find_file(file_name, sub_path='**'):

    path_pattern = f"{PROJECT_ROOT_PATH}/{sub_path}/{file_name}"

    file_path = glob.glob(path_pattern)

    if not file_path:
        raise Exception(f"Could not find {file_name}")
    else:
        return file_path[0]

def get_role_names(roles_file):
    with open(roles_file, "r") as stream:
        try:
            yaml_loaded = yaml.safe_load(stream)
            role_names = list()
            for obj in yaml_loaded:
                for k in obj.keys():
                    role_names.append(k.upper())
            return role_names

        except yaml.YAMLError as exc:
            print(exc)

def get_schemas_names(file):
    schema_list = list()
    with open(file, "r") as stream:
        try:
            yaml_loaded = yaml.safe_load(stream)
            for database in yaml_loaded:
                for database, v in database.items():
                    if 'schemas' in v:
                        for schema in v['schemas']:
                            fully_qualified_schema = f"{database.upper()}.{schema}"
                            schema_list.append(fully_qualified_schema)

            return schema_list

        except yaml.YAMLError as exc:
            print(exc)

def get_warehouses(file):
    warehouses_list = list()
    with open(file, "r") as stream:
        try:
            yaml_loaded = yaml.safe_load(stream)
            
            for wh in yaml_loaded:
                warehouses_list = get_warehouses_and_attributes(wh, warehouses_list)
            
            return warehouses_list

        except yaml.YAMLError as exc:
            print(exc)

def get_warehouses_and_attributes(wh, wh_list):
    wh_dict = dict()
    for k, v in wh.items():
        wh_dict["name"] = k
        if isinstance(v, dict):
            wh_dict["parameters"] = dict()
            for param, value in v.items():
                wh_dict["parameters"][param] = value
    
    wh_list.append(wh_dict)

    return wh_list

def run_macro(macro_name, args, target):
    dbt_project_path = Path(find_file("dbt_project.yml")).parent
    if not dbt_project_path:
        raise Exception(f"Could not locate dbt_project.yml path")
    if args:
        if target:
            command = ["dbt", "run-operation", macro_name, "--args", args, "--target", target]
        else:
            command = ["dbt", "run-operation", macro_name, "--args", args]
    else:
        if target:
            command = ["dbt", "run-operation", macro_name,  "--target", target]
        else:
            command = ["dbt", "run-operation", macro_name]

    subprocess.run(command, cwd=dbt_project_path)

def main(is_dry_run, snowflake_objects, target):

    if (snowflake_objects == 'all') or (snowflake_objects == 'roles'):
        roles_file = find_file("roles.yml", sub_path='secure')

        permifrost_roles = get_role_names(roles_file)

        permifrost_roles_args = {
            "permifrost_role_list": ",".join(permifrost_roles),
            "dry_run": is_dry_run
        }

        run_macro(
            macro_name = DBT_MACRO_NAME_ROLE_COMPARISON,
            args = json.dumps(permifrost_roles_args),
            target = target
        )

    if (snowflake_objects == 'all') or (snowflake_objects == 'schemas'):
        schemas_file = find_file("databases.yml", sub_path='secure')
        permifrost_schemas = get_schemas_names(schemas_file)

        permifrost_schemas_args = {
            "permifrost_schema_list": ",".join(permifrost_schemas),
            "dry_run": is_dry_run
        }

        run_macro(
            macro_name=DBT_MACRO_NAME_SCHEMA_COMPARISON,
            args=json.dumps(permifrost_schemas_args),
            target = target
        )

    if (snowflake_objects == 'all') or (snowflake_objects == 'warehouses'):
        warehouses_file = find_file("warehouses.yml", sub_path='secure')
        permifrost_warehouses = get_warehouses(find_file("warehouses.yml"))

        permifrost_warehouses_args = {
            "permifrost_warehouse_list": permifrost_warehouses,
            "dry_run": is_dry_run
        }

        run_macro(
            macro_name=DBT_MACRO_NAME_WAREHOUSE_COMPARISON,
            args=json.dumps(permifrost_warehouses_args),
            target = target
        )

if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)

    try:
        parser = argparse.ArgumentParser(
            description="Used to synchonize objects Snowflake and Permifrost."
        )

        parser.add_argument(
            "--dry-run",
            dest="is_dry_run",
            action="store_true",
            help="Runs command without applying changes"
        )

        parser.add_argument(
                    "-s",
                    "--snowflake_objects",
                    dest = 'snowflake_objects',
                    default = 'all',
                    const = 'all',
                    nargs = '?',
                    choices = ['all', 'roles', 'schemas', 'warehouses'],
                    help = "Defines which objects to run (default: %(default)s)"
        )

        parser.add_argument(
                    "-t",
                    "--target",
                    action='store', 
                    type=str,
                    help = 'dbt target to use for credentials'
        )

        args = vars(parser.parse_args())

        main(**args)

    except Exception as ex:
        logging.info(ex)
        exit(1)
