#!/usr/bin/env -S uv run
# /// script
# dependencies = [
#   "dlt[snowflake, parquet]==1.21.0",
#   "pandas==2.2.2",
#   "psutil~=6.0.0",
#   "requests",
# ]
# ///
# """Loads earthquake data into Snowflake"""
import dlt
import requests
from datetime import date, timedelta
import argparse
from utils.datacoves_utils import pipelines_dir

parser = argparse.ArgumentParser(description='Process start date.')

# Add the arguments
parser.add_argument('--start-date', type=str, required=True, help='The start date to fetch earthquake data')

# Parse the arguments
args = parser.parse_args()
start_date = args.start_date

# start_date = date.today() - timedelta(7)
end_date = date.today()

# Fail fast if the requested range is very large (likely a typo in the year)
try:
    start_date_obj = date.fromisoformat(start_date)
    days_back = (end_date - start_date_obj).days
    if days_back > 30:
        raise ValueError(
            f"start-date {start_date} is {days_back} days before today. "
            "This is more than 30 days and is likely unintended. "
            "Please choose a start-date within the last 30 days or adjust the script if you "
            "really want a larger backfill."
        )
except ValueError:
    # If the date is invalid, let the later parsing fail normally or re-raise
    raise

def build_url(start: date, end: date) -> str:
    return (
        "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson"
        f"&starttime={start}&endtime={end}"
    )

url = build_url(start_date, end_date)
print(f"Fetching data from: {url}")

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
    """Yield earthquake features in small date chunks to stay under USGS limits."""

    # USGS caps results at 20k events per request; use 1‑day chunks to be safe
    chunk_size_days = 1
    current_start = date.fromisoformat(start_date)

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=chunk_size_days), end_date)
        chunk_url = build_url(current_start, current_end)
        print(f"Fetching data from: {chunk_url}")

        response = requests.get(chunk_url, timeout=30)

        if not response.ok:
            raise RuntimeError(
                f"USGS API error {response.status_code} for {current_start}–{current_end}: "
                f"{response.text[:500]}"
            )

        try:
            data = response.json()
        except ValueError:
            raise RuntimeError(
                "USGS API did not return JSON "
                f"for {current_start}–{current_end}. Status: {response.status_code}, "
                f"Content-Type: {response.headers.get('Content-Type')}, "
                f"Body: {response.text[:500]}"
            )

        features = data.get("features", [])
        if features:
            # dlt can handle lists; yield per chunk
            yield features

        current_start = current_end

if __name__ == "__main__":
    datacoves_snowflake = dlt.destinations.snowflake(
        destination_name="datacoves_snowflake",
        enable_dataset_name_normalization=False
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
