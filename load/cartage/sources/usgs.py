"""USGS earthquakes, incremental on the event time (replaces the DAG's --start-date)."""
from datetime import date, datetime, timedelta, timezone

import dlt
import requests

URL = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start}&endtime={end}"


@dlt.resource(name="earthquakes")
def earthquakes(start_date: str | None = None, days_back: int = 7, max_days_back: int = 30,
                time=dlt.sources.incremental("properties.time")):
    """Fetch from start_date, else the last loaded event (Cartage state), else today - days_back."""
    end = date.today()
    if start_date:
        start = date.fromisoformat(start_date)
    elif time.start_value:
        start = datetime.fromtimestamp(time.start_value / 1000, timezone.utc).date()
    else:
        start = end - timedelta(days=days_back)
    if (end - start).days > max_days_back:
        raise ValueError(f"start date {start} is {(end - start).days} days back (max {max_days_back}); "
                         "pass start_date or raise max_days_back for a larger backfill")

    while start < end:  # USGS caps a request at 20k events: fetch one day at a time
        stop = min(start + timedelta(days=1), end)
        response = requests.get(URL.format(start=start, end=stop), timeout=30)
        if not response.ok:
            raise RuntimeError(f"USGS API error {response.status_code} for {start}–{stop}: {response.text[:500]}")
        if features := response.json().get("features", []):
            yield features
        start = stop
