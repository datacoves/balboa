"""Snowflake post-load steps, used as `after_load` hooks: Cartage calls them with the dlt pipeline after a load.
Same as load/dlt/utils/datacoves_utils.py, minus pipelines_dir (Cartage keeps dlt state itself)."""


def _is_snowflake(pipeline) -> bool:
    if pipeline.destination.destination_type.endswith("snowflake"):
        return True
    print(f"Skipped: {pipeline.destination.destination_type} is not Snowflake")
    return False


def apply_pii_tag(pipeline, table: str, columns: list[str]):
    """Apply the GOVERNANCE.TAGS.PII tag to specified columns after loading."""
    if not _is_snowflake(pipeline):
        return
    with pipeline.sql_client() as client:
        for col in columns:
            client.execute_sql(
                f"ALTER TABLE {pipeline.dataset_name}.{table} "
                f"ALTER COLUMN {col} SET TAG GOVERNANCE.TAGS.PII = 'true'"
            )
    print(f"PII tag applied to {table}: {', '.join(columns)}")


def enable_change_tracking(pipeline, tables: list[str]):
    """Enable CHANGE_TRACKING on Snowflake tables for Dynamic Table support."""
    if not _is_snowflake(pipeline):
        return
    with pipeline.sql_client() as client:
        for table in tables:
            client.execute_sql(
                f"ALTER TABLE {pipeline.dataset_name}.{table} SET CHANGE_TRACKING = TRUE"
            )
    print(f"CHANGE_TRACKING enabled on: {', '.join(tables)}")
