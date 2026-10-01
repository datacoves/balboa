"""Files downloaded over HTTP."""
import json

import dlt
import pandas as pd
import requests


@dlt.source
def csv_files(files: dict[str, str]):
    """One resource (table) per CSV: {table_name: url}."""
    def read(url):
        yield pd.read_csv(url)

    return [dlt.resource(read(url), name=name) for name, url in files.items()]


@dlt.resource(name="country_polygons")
def country_polygons(url: str = "https://datahub.io/core/geo-countries/_r/-/data/countries.geojson"):
    """One row per country: flattened properties, geometry as a JSON string."""
    for feature in requests.get(url, timeout=60).json().get("features", []):
        geometry = feature.get("geometry", {})
        yield {**feature.get("properties", {}), "geometry_type": geometry.get("type"), "geometry": json.dumps(geometry)}
