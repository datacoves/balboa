import os
import snowflake.connector

from titan.blueprint import Blueprint, print_plan
from titan.resources import Grant, Role, Warehouse
import yaml


warehouse = Warehouse(
    name="transforming",
    warehouse_size="large",
    auto_suspend=60,
)

def read_config(filename: str):
    with open(os.path.join(os.path.dirname(__file__), f'{filename}.yml'), 'r') as file:
        return yaml.safe_load(file)

# Process the data from the YAML file
warehouses = []
for warehouse_info in read_config('warehouses'):

    name, config = next(iter(warehouse_info.items()))

    print(name)
    print(config)
    warehouse = Warehouse(
        name=name,
        warehouse_size=config['size'],
        auto_suspend=config['auto_suspend'],
        initially_suspended=config['initially_suspended'],
    )
    print(f"{warehouse=}")
    warehouses.append(warehouse)    

bp = Blueprint(resources=[
    # role,
    *warehouses,
    # usage_grant,
])


connection_params = {
    "account": "CQBVXGL-lda41570",
    "user": "mucio",
    "password": "CKzH9SaLdMG9Snp",
    "role": "SYSADMIN",
}
session = snowflake.connector.connect(**connection_params)

plan = bp.plan(session)
print_plan(plan)