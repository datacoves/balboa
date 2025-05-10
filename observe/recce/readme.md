# Setup

Install Python dependencies

```
pip install -r requirements.txt
```

Install *VS Code extension* for DAG preview in summary
```
Markdown Preview Mermaid Support
v1.26.0
```

Add dbt dependencies to packages.yml (check for latest version)
```
  - package: data-mie/dbt_profiler
    version: 0.8.4
  - package: dbt-labs/audit_helper
    version: 0.12.1
```

# Run Recce Server
`./start_recce_server.sh`

# Generating report
`./run_recce.sh`

This will generate a recce_state.json file

# Generate markdown summary
`./summary_recce.sh`

Then right click and preview the file.
