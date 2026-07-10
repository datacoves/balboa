#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

uv run --with="dlt[hub]" --with="marimo" --with="pyarrow" --with="ibis-framework[snowflake]" python -c "
from dlt._workspace.helpers.dashboard.runner import run_dashboard
run_dashboard(port=8501, host='0.0.0.0', headless=True)
"
