#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "dlt[snowflake, parquet]==1.21.0",
#   "enlighten~=1.12.4",
#   "psutil~=6.0.0",
#   "connectorx==0.4.1",
#   "pandas==2.2.2",
# ]
# ///
"""Loads world population CSV data to Snowflake RAW database"""
import dlt
import pandas as pd
from utils.datacoves_utils import pipelines_dir

@dlt.resource(write_disposition="replace", table_name="country_populations")
def country_populations():
    url = "https://raw.githubusercontent.com/datasets/population/master/data/population.csv"
    df = pd.read_csv(url)
    yield df

@dlt.source
def country_populations_source():
    return [country_populations]

if __name__ == "__main__":
    datacoves_snowflake = dlt.destinations.snowflake(
        destination_name="datacoves_snowflake",
        database="raw"
    )

    pipeline = dlt.pipeline(
        progress="log",
        pipeline_name="world_population_data",
        destination=datacoves_snowflake,
        pipelines_dir=pipelines_dir,
        dataset_name="raw"
    )

    load_info = pipeline.run([
        country_populations_source()
    ])

    print(load_info)
