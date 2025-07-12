# Install the SQLMesh VS Code Extension

https://marketplace.visualstudio.com/items?itemName=tobikodata.sqlmesh

# Migrating from dbt to SQLMesh

Follow this process to migrate a project from dbt to SQLMesh

https://www.youtube.com/watch?v=CDP1pKS2z6U

## Use dbt to sqlmesh converter

https://github.com/astronautyates/dbt_to_sqlmesh_converter

## Install the python extension

```
uv venv
uv pip install -r https://raw.githubusercontent.com/astronautyates/dbt_to_sqlmesh_converter/refs/heads/main/requirements.txt
curl -s https://raw.githubusercontent.com/astronautyates/dbt_to_sqlmesh_converter/refs/heads/main/dbt_to_sqlmesh.py | uv run - /config/workspace/transform /config/workspace/transform_sqlmesh
```

## Test the converted project

```
cd /config/workspace/transform_sqlmesh
uv run --with "sqlmesh[dbt,lsp,snowflake]" sqlmesh plan
```
