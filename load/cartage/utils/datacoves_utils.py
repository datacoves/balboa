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


def record_row_counts(pipeline, table: str = "_dlt_load_counts"):
    """Record how many rows each table received in this run, from dlt's trace: one row per table, with 0 (and no
    load_id) for the pipeline's tables that got no rows."""
    normalize_info = pipeline.last_trace.last_normalize_info
    rows = [
        (load_id, pipeline.pipeline_name, table_name, table_metrics.items_count)
        for load_id, metrics in (normalize_info.metrics if normalize_info else {}).items()
        for table_name, table_metrics in metrics[0]["table_metrics"].items()
        if not table_name.startswith("_dlt")
    ]
    # Tables in the pipeline's schema that got no rows: dlt loads nothing for them, so there is no load_id
    loaded = {row[2] for row in rows}
    rows += [
        (None, pipeline.pipeline_name, table_name, 0)
        for table_name in pipeline.default_schema.data_table_names()
        if table_name not in loaded
    ]
    with pipeline.sql_client() as client:
        qualified_table = client.make_qualified_table_name(table)
        client.execute_sql(
            f"CREATE TABLE IF NOT EXISTS {qualified_table} "
            "(load_id VARCHAR, pipeline_name VARCHAR, table_name VARCHAR, row_count BIGINT, inserted_at TIMESTAMP)"
        )
        for row in rows:
            client.execute_sql(
                f"INSERT INTO {qualified_table} VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)", *row
            )
    print(f"Row counts recorded in {table}: " + ", ".join(f"{r[2]}={r[3]}" for r in rows))
