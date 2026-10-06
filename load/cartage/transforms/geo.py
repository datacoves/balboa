"""GeoJSON features → flat rows."""
import json


def flatten_feature(feature: dict) -> dict:
    """The feature's properties as columns, plus geometry_type and geometry (a JSON string)."""
    geometry = feature.get("geometry") or {}
    return {**feature.get("properties", {}), "geometry_type": geometry.get("type"), "geometry": json.dumps(geometry)}
