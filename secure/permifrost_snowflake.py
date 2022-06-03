import glob
import json
import subprocess
import yaml

from pathlib import Path

DBT_MACRO_NAME_OBJ_NOT_IN_PERMIFROST = "objects_not_in_permifrost"
DBT_MACRO_NAME_GRANT_DB_CREATE = "grant_database_creation_to_dbt_prd"
PROJECT_ROOT_PATH = ".."

permifrost_databases = list()
permifrost_warehouses = list()
permifrost_roles = list()
permifrost_schemas = list()


def find_file(root_path, file_name):
    for file in glob.glob(f"{str(root_path)}/**/{file_name}"):
        if file:
            return file
        else:
            return False


def get_schemas(role_dict, schema_list):
    for k, v in role_dict.items():
        if isinstance(v, dict):
            if k == "schemas":
                if isinstance(v, list):
                    for schema in v:
                        if "*" not in schema and schema not in schema_list:
                            schema_list.append(schema.upper())
                if isinstance(v, dict):
                    for privilege, schemas in v.items():
                        for schema in schemas:
                            if "*" not in schema and schema not in schema_list:
                                schema_list.append(schema.upper())
            else:
                return get_schemas(v, schema_list)

    return schema_list


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


def get_schemas_names(file):
    schema_list = list()
    with open(file, "r") as stream:
        try:
            yaml_loaded = yaml.safe_load(stream)
            for role in yaml_loaded:
                schema_list = get_schemas(role, schema_list)
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


def get_object_names(file):
    with open(file, "r") as stream:
        try:
            yaml_loaded = yaml.safe_load(stream)
            obj_names = list()
            for obj in yaml_loaded:
                for k in obj.keys():
                    obj_names.append(k.upper())
            return obj_names

        except yaml.YAMLError as exc:
            print(exc)


def run_macro(macro_name, args):
    dbt_project_path = Path(find_file(PROJECT_ROOT_PATH, "dbt_project.yml")).parent
    if not dbt_project_path:
        raise Exception(f"Could not locate dbt_project.yml path")
    if args:
        command = ["dbt", "run-operation", macro_name, "--args", args]
    else:
        command = ["dbt", "run-operation", macro_name]
    subprocess.run(command, cwd=dbt_project_path)


permifrost_databases = get_object_names(find_file(PROJECT_ROOT_PATH, "databases.yml"))
permifrost_warehouses = get_warehouses(find_file(PROJECT_ROOT_PATH, "warehouses.yml"))
roles_file = find_file(PROJECT_ROOT_PATH, "roles.yml")
permifrost_roles = get_object_names(roles_file)
permifrost_schemas = get_schemas_names(roles_file)

permifrost_databases_args = {
    "permifrost_list": ",".join(permifrost_databases),
    "obj_type": "Databases",
}
permifrost_warehouses_args = {
    "permifrost_list": permifrost_warehouses,
    "obj_type": "Warehouses",
}
permifrost_roles_args = {
    "permifrost_list": ",".join(permifrost_roles),
    "obj_type": "Roles",
}
permifrost_schemas_args = {
    "permifrost_list": ",".join(permifrost_schemas),
    "obj_type": "Schemas",
}


run_macro(
    macro_name=DBT_MACRO_NAME_OBJ_NOT_IN_PERMIFROST,
    args=json.dumps(permifrost_roles_args),
)

# grant create database on account to role transformer_dbt_prd
run_macro(DBT_MACRO_NAME_GRANT_DB_CREATE, args=None)

run_macro(
    macro_name=DBT_MACRO_NAME_OBJ_NOT_IN_PERMIFROST,
    args=json.dumps(permifrost_databases_args),
)
run_macro(
    macro_name=DBT_MACRO_NAME_OBJ_NOT_IN_PERMIFROST,
    args=json.dumps(permifrost_schemas_args),
)
run_macro(
    macro_name=DBT_MACRO_NAME_OBJ_NOT_IN_PERMIFROST,
    args=json.dumps(permifrost_warehouses_args),
)
