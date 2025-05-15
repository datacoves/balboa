#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "dlt[snowflake, parquet]==1.9.0",
#   "psutil~=6.0.0",
#   "pandas==2.2.2",
# ]
# ///
"""Loads a CSV file to Snowflake"""
import dlt
import pandas as pd
from utils.datacoves_utils import pipelines_dir

@dlt.resource(write_disposition="replace")
def country_polygons():
    import requests
    import json

    country_polygons_geojson = "https://datahub.io/core/geo-countries/_r/-/data/countries.geojson"
    response = requests.get(country_polygons_geojson)
    geo_data = response.json()

    # Extract features from GeoJSON
    features = geo_data.get("features", [])

    # Create rows for each country
    for feature in features:
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})

        # Create a row with flattened properties and geometry as JSON
        row = {
            **properties,
            "geometry_type": geometry.get("type"),
            "geometry": json.dumps(geometry)  # Store geometry as JSON string
        }

        yield row

@dlt.source
def country_polygons_source():
    return [country_polygons]

if __name__ == "__main__":
    datacoves_snowflake = dlt.destinations.snowflake(
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
