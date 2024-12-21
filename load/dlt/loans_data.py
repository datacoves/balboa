#!/usr/bin/env -S uv run --cache-dir /tmp/.uv_cache
# /// script
# dependencies = [
#   "dlt[snowflake, parquet]==1.5.0",
#   "enlighten~=1.12.4",
#   "psutil~=6.0.0",
#   "pandas==2.2.2",
# ]
# ///
"""Loads a CSV file to Snowflake"""
import dlt
import pandas as pd
from utils.datacoves_snowflake import db_config
from utils.datacoves import pipelines_dir

@dlt.resource(write_disposition="replace")
def personal_loans():
    personal_loans = "https://datacoves-sample-data-public.s3.us-west-2.amazonaws.com/PERSONAL_LOANS.csv"
    df = pd.read_csv(personal_loans)
    yield df

@dlt.resource(write_disposition="replace")
def zip_coordinates():
    zip_coordinates = "https://datacoves-sample-data-public.s3.us-west-2.amazonaws.com/ZIP_COORDINATES.csv"
    df = pd.read_csv(zip_coordinates)
    yield df

@dlt.source
def personal_loans_source():
    return [personal_loans]

@dlt.source
def zip_coordinates_source():
    return [zip_coordinates]

if __name__ == "__main__":
    datacoves_snowflake = dlt.destinations.snowflake(
        db_config,
        destination_name="datacoves_snowflake"
    )

    pipeline = dlt.pipeline(
        progress = "enlighten",
        pipeline_name = "loans",
        destination = datacoves_snowflake,
        pipelines_dir = pipelines_dir,

        # dataset_name is the target schema name
        dataset_name="loans"
    )

    load_info = pipeline.run([
            personal_loans(),
            zip_coordinates()
        ])

    print(load_info)
