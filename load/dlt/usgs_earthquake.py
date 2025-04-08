#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "dlt[snowflake, parquet]==1.9.0",
#   "pandas==2.2.2",
#   "requests",
# ]
# ///
# """Loads earthquake data into Snowflake"""
import dlt
import requests
from datetime import date, timedelta
from utils.datacoves_snowflake import db_config
from utils.datacoves import pipelines_dir
import argparse

parser = argparse.ArgumentParser(description='Process start date.')

# Add the arguments
parser.add_argument('--start-date', type=str, required=True, help='The start date to fetch earthquake data')

# Parse the arguments
args = parser.parse_args()
start_date = args.start_date

# start_date = date.today() - timedelta(7)
end_date = date.today() - timedelta(10)
url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"

@dlt.resource(
    table_name='earthquakes',
    primary_key="id",
    write_disposition="merge",
    columns={
        "id": {"data_type": "text"},
        "properties": {"data_type": "json"},
        "geometry": {"data_type": "json"},
        "type": {"data_type": "text"}
    }
)
def earthquake_data():
    response = requests.get(url)
    features = response.json()["features"]
    yield features

if __name__ == "__main__":
    datacoves_snowflake = dlt.destinations.snowflake(
        db_config,
        destination_name="datacoves_snowflake"
    )

    pipeline = dlt.pipeline(
        progress = "log",
        pipeline_name = "earthquake_pipeline",
        destination = datacoves_snowflake,
        pipelines_dir = pipelines_dir,

        # dataset_name is the target schema name
        dataset_name="usgs__earthquake_data"
    )

    load_info = pipeline.run([
            earthquake_data()
        ])

    print(load_info)
