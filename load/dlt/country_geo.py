#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "dlt[snowflake, parquet]==1.9.0",
#   "pandas==2.2.2",
# ]
# ///
"""Loads a CSV file to Snowflake"""
import dlt
import pandas as pd
from utils.datacoves_snowflake import db_config
from utils.datacoves import pipelines_dir

@dlt.resource(
    write_disposition="replace"
)
def country_polygons():
    country_polygons_geojson = "https://datahub.io/core/geo-countries/_r/-/data/countries.geojson"
    df = pd.read_json(country_polygons_geojson)
    yield df

@dlt.source
def country_polygons_source():
    return [country_polygons]

if __name__ == "__main__":
    datacoves_snowflake = dlt.destinations.snowflake(
        db_config,
        destination_name="datacoves_snowflake"
    )

    pipeline = dlt.pipeline(
        progress = "log",
        pipeline_name = "polygons",
        destination = datacoves_snowflake,
        pipelines_dir = pipelines_dir,

        # dataset_name is the target schema name
        dataset_name="country_geo"
    )

    load_info = pipeline.run([
            country_polygons_source()
        ])

    print(load_info)
