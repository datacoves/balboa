import os

# check if this is running in VS Code
in_vs_code = os.getenv('DATACOVES__USER_SLUG', None)
pipelines_dir = ''
if in_vs_code:
    pipelines_dir = os.path.join('/config','.dlt','pipelines')
    print(f"pipelines_dir set to: {pipelines_dir}")
else:
    pipelines_dir = os.path.join('/tmp','.dlt','pipelines')
    print(f"pipelines_dir set to: {pipelines_dir}")


def apply_pii_tag(pipeline, table: str, columns: list[str]):
    """Apply the GOVERNANCE.TAGS.PII tag to specified columns after loading.

    Args:
        pipeline: A dlt pipeline instance with a Snowflake destination.
        table: Table name containing the columns to tag.
        columns: List of column names to apply the PII tag to.
    """
    with pipeline.sql_client() as client:
        for col in columns:
            client.execute_sql(
                f"ALTER TABLE {pipeline.dataset_name}.{table} "
                f"ALTER COLUMN {col} SET TAG GOVERNANCE.TAGS.PII = 'true'"
            )
    print(f"PII tag applied to {table}: {', '.join(columns)}")


def enable_change_tracking(pipeline, tables: list[str]):
    """Enable CHANGE_TRACKING on Snowflake tables for Dynamic Table support.

    Args:
        pipeline: A dlt pipeline instance with a Snowflake destination.
        tables: List of table names to enable change tracking on.
    """
    with pipeline.sql_client() as client:
        for table in tables:
            client.execute_sql(
                f"ALTER TABLE {pipeline.dataset_name}.{table} SET CHANGE_TRACKING = TRUE"
            )
    print(f"CHANGE_TRACKING enabled on: {', '.join(tables)}")
