# dlt Workspace Dashboard

How to run the `dlt dashboard` (marimo-based UI for browsing pipelines, schemas,
and destination data) so it's reachable on an external port.

## Run

```bash
load/dlt/run_dashboard.sh
```

Then open `http://<external-ip>:8501`.

To change the port, edit `port=8501` in `load/dlt/run_dashboard.sh`.

## Stopping it

```bash
pkill -f "marimo run"
```
