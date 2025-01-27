#!/usr/bin/env -S uv run
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
def us_population():
    us_population = "https://raw.githubusercontent.com/dataprofessor/dashboard-v3/master/data/us-population-2010-2019.csv"
    df = pd.read_csv(us_population)
    yield df

@dlt.source
def us_population_source():
    return [us_population]

if __name__ == "__main__":
    datacoves_snowflake = dlt.destinations.snowflake(
        db_config,
        destination_name="datacoves_snowflake"
    )

    pipeline = dlt.pipeline(
        progress = "log",
        pipeline_name = "loans",
        destination = datacoves_snowflake,
        pipelines_dir = pipelines_dir,

        # dataset_name is the target schema name
        dataset_name="us_population"
    )

    load_info = pipeline.run([
            us_population()
        ])

    print(load_info)
